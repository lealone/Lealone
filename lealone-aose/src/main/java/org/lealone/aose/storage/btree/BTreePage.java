/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.aose.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.lealone.aose.storage.AOStorageService;
import org.lealone.common.compress.Compressor;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.type.StorageDataType;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * File format: page length (including length): int check value:
 * varInt number of keys: varInt type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt) keys leaf: values (one for each key) node:
 * children (1 more than keys)
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreePage {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;
    public static final boolean DEBUG = true;

    /**
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private final BTreeMap<?, ?> map;
    private long pos;

    /**
     * The total entry count of this page and all children.
     */
    private long totalCount;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used.
     */
    private int memory;

    /**
     * The keys.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] keys;

    /**
     * The values.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] values;

    /**
     * The child page references.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private PageReference[] children;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;

    String remoteHostId;
    List<String> replicationHostIds;
    LeafPageMovePlan leafPageMovePlan;

    BTreePage(BTreeMap<?, ?> map) {
        this.map = map;
    }

    /**
     * Get the position of the page
     * 
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    /**
    * Get the key at the given index.
    * 
    * @param index the index
    * @return the key
    */
    public Object getKey(int index) {
        return keys[index];
    }

    /**
     * Get the value at the given index.
     * 
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        return values[index];
    }

    /**
     * Get the number of keys in this page.
     * 
     * @return the number of keys
     */
    public int getKeyCount() {
        return keys.length;
    }

    /**
     * Get the child page at the given index.
     * 
     * @param index the index
     * @return the child page
     */
    public BTreePage getChildPage(int index) {
        PageReference ref = children[index];
        return ref.page != null ? ref.page : map.storage.readPage(ref, ref.pos);
    }

    /**
     * Check whether this is a leaf page.
     * 
     * @return true if it is a leaf
     */
    public boolean isLeaf() {
        return children == null;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     * 
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        Object[] k = keys;
        StorageDataType keyType = map.getKeyType();
        while (low <= high) {
            int compare = keyType.compare(key, k[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                cachedCompare = x + 1;
                return x;
            }
            x = (low + high) >>> 1;
        }
        cachedCompare = low;
        return -(low + 1);

        // regular binary search (without caching)
        // int low = 0, high = keys.length - 1;
        // while (low <= high) {
        // int x = (low + high) >>> 1;
        // int compare = map.compare(key, keys[x]);
        // if (compare > 0) {
        // low = x + 1;
        // } else if (compare < 0) {
        // high = x - 1;
        // } else {
        // return x;
        // }
        // }
        // return -(low + 1);
    }

    boolean needSplit() {
        return memory > map.storage.getPageSplitSize() && keys.length > 1;
    }

    /**
     * Split the page. This modifies the current page.
     * 
     * @param at the split index
     * @return the page with the entries after the split index
     */
    BTreePage split(int at) {
        return isLeaf() ? splitLeaf(at) : splitNode(at);
    }

    private BTreePage splitLeaf(int at) { // 小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        totalCount = a;
        BTreePage newPage = create(map, bKeys, bValues, null, bKeys.length, 0);
        newPage.replicationHostIds = replicationHostIds;
        recalculateMemory();
        return newPage;
    }

    private BTreePage splitNode(int at) { // at对应的key只放在父节点中
        int a = at, b = keys.length - a;

        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        // children的长度要比keys的长度多1并且右边所有leaf的key都大于或等于at下标对应的key
        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        long t = 0;
        for (PageReference x : aChildren) {
            t += x.count;
        }
        totalCount = t;
        t = 0;
        for (PageReference x : bChildren) {
            t += x.count;
        }
        BTreePage newPage = create(map, bKeys, null, bChildren, t, 0);
        recalculateMemory();
        return newPage;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     * 
     * @return the number of key-value pairs
     */
    public long getTotalCount() {
        if (ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
            } else {
                for (PageReference x : children) {
                    check += x.count;
                }
            }
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Expected: {0} got: {1}", check,
                        totalCount);
            }
        }
        return totalCount;
    }

    /**
     * Replace the key at an index in this page.
     * 
     * @param index the index
     * @param key the new key
     */
    public void setKey(int index, Object key) {
        // this is slightly slower:
        // keys = Arrays.copyOf(keys, keys.length);
        keys = keys.clone();
        Object old = keys[index];
        StorageDataType keyType = map.getKeyType();
        int mem = keyType.getMemory(key);
        if (old != null) {
            mem -= keyType.getMemory(old);
        }
        addMemory(mem);
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     * 
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length);
        values = values.clone();
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    /**
     * Replace the child page.
     * 
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, BTreePage c) {
        if (c == null) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(null, 0, 0);
            children[index] = ref;
            totalCount -= oldCount;
        } else if (c != children[index].page || c.getPos() != children[index].pos) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(c, c.pos, c.totalCount);
            children[index] = ref;
            totalCount += c.totalCount - oldCount;
        }
    }

    /**
     * Insert a key-value pair into this leaf.
     * 
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        addMemory(map.getKeyType().getMemory(key) + map.getValueType().getMemory(value));
    }

    /**
     * Insert a child page into this node.
     * 
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, BTreePage childPage) {
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(childPage, childPage.getPos(), childPage.totalCount);
        children = newChildren;

        totalCount += childPage.totalCount;
        addMemory(map.getKeyType().getMemory(key) + DataUtils.PAGE_MEMORY_CHILD);
    }

    /**
     * Remove the key and value (or child) at the given index.
     * 
     * @param index the index
     */
    public void remove(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        Object old = keys[keyIndex];
        addMemory(-map.getKeyType().getMemory(old));
        Object[] newKeys = new Object[keyLength - 1];
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;

        if (values != null) {
            old = values[index];
            addMemory(-map.getValueType().getMemory(old));
            Object[] newValues = new Object[keyLength - 1];
            DataUtils.copyExcept(values, newValues, keyLength, index);
            values = newValues;
            totalCount--;
        }
        if (children != null) {
            addMemory(-DataUtils.PAGE_MEMORY_CHILD);
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }
    }

    /**
     * Read the page from the buffer.
     * 
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean isLeaf) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, maxLength,
                    pageLength);
        }
        buff.limit(start + pageLength);
        if (isLeaf) {
            int keyLength = buff.getInt();
            if (keyLength != 0) {
                map.getKeyType().read(buff); // read first key
            }
        }
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }
        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[keyLength + 1];
            long[] p = new long[keyLength + 1];
            for (int i = 0; i <= keyLength; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= keyLength; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) == DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getBTreeStorage().getCompressorHigh();
            } else {
                compressor = map.getBTreeStorage().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(), buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, keyLength);
        if (!node) {
            values = new Object[keyLength];
            map.getValueType().read(buff, values, keyLength);
            totalCount = keyLength;
            replicationHostIds = readReplicationHostIds(buff);
        }
        recalculateMemory();
    }

    void writeLeaf(DataBuffer buff, boolean remote) {
        writeReplicationHostIds(buff);
        buff.put((byte) (remote ? 1 : 0));
        if (!remote) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = map.getValueType();
            buff.putInt(keys.length);
            for (int i = 0; i < keys.length; i++) {
                kt.write(buff, keys[i]);
                vt.write(buff, values[i]);
            }
        }
    }

    static BTreePage readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        List<String> replicationHostIds = readReplicationHostIds(page);
        boolean remote = page.get() == 1;
        BTreePage p;

        if (remote) {
            p = BTreePage.createEmpty(map);
        } else {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = map.getValueType();
            int length = page.getInt();
            Object[] keys = new Object[length];
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                keys[i] = kt.read(page);
                values[i] = vt.read(page);
            }
            p = BTreePage.create(map, keys, values, null, length, 0);
        }

        p.replicationHostIds = replicationHostIds;
        return p;
    }

    private void writeReplicationHostIds(DataBuffer buff) {
        if (replicationHostIds == null || replicationHostIds.isEmpty())
            buff.putInt(0);
        else {
            buff.putInt(replicationHostIds.size());
            for (String id : replicationHostIds) {
                ValueString.type.write(buff, id);
            }
        }
    }

    private static List<String> readReplicationHostIds(ByteBuffer buff) {
        int length = buff.getInt();
        List<String> replicationHostIds = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
            replicationHostIds.add(ValueString.type.read(buff));

        if (replicationHostIds.isEmpty())
            replicationHostIds = null;

        return replicationHostIds;
    }

    /**
     * Store the page and update the position.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    private int write(BTreeChunk chunk, DataBuffer buff) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = children != null ? DataUtils.PAGE_TYPE_NODE : DataUtils.PAGE_TYPE_LEAF;
        buff.putInt(0);
        if (type == DataUtils.PAGE_TYPE_LEAF) { // write first key
            buff.putInt(keyLength);
            if (keyLength != 0) {
                map.getKeyType().write(buff, keys[0]);
            }
            // 不能这样，ValueNull.INSTANCE不会自动转成其他类型
            // if (keyLength == 0) {
            // map.getKeyType().write(buff, ValueNull.INSTANCE);
            // } else {
            // map.getKeyType().write(buff, keys[0]);
            // }
        }
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            writeChildrenPositions(buff);
            for (int i = 0; i <= keyLength; i++) {
                buff.putVarLong(children[i].count); // count可能不大，所以用VarLong能节省一些空间
            }
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        if (type == DataUtils.PAGE_TYPE_LEAF) {
            map.getValueType().write(buff, values, keyLength);
            writeReplicationHostIds(buff);
        }
        BTreeStorage storage = map.getBTreeStorage();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = storage.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = storage.getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = storage.getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).put((byte) (type + compressType));
                    buff.position(compressStart).putVarInt(expLen - compLen).put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).putShort(checkPos, (short) check);
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        chunk.pagePositions.add(pos);
        chunk.pageLengths.add(pageLength);
        storage.cachePage(pos, this, getMemory());
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            storage.cachePage(pos, this, getMemory());
        }
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        if (chunk.sumOfPageLength > BTreeChunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", BTreeChunk.MAX_SIZE, chunk.sumOfPageLength);
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            storage.removePage(pos, memory);
        }
        return typePos + 1;
    }

    private void writeChildrenPositions(DataBuffer buff) {
        int len = keys.length;
        for (int i = 0; i <= len; i++) {
            buff.putLong(children[i].pos); // pos通常是个很大的long，所以不值得用VarLong
        }
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff);
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                BTreePage p = children[i].page;
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff);
                    children[i] = new PageReference(p, p.getPos(), p.totalCount);
                }
            }
            int old = buff.position();
            buff.position(patch);
            writeChildrenPositions(buff);
            buff.position(old);
        }
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
        if (isLeaf()) {
            return;
        }
        int len = children.length;
        for (int i = 0; i < len; i++) {
            PageReference ref = children[i];
            if (ref.page != null) {
                if (ref.page.getPos() == 0) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos, ref.count);
            }
        }
    }

    public int getRawChildPageCount() {
        return children.length;
    }

    public int getMemory() {
        if (ASSERT) {
            int mem = memory;
            recalculateMemory();
            if (mem != memory) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Memory calculation error");
            }
        }
        return memory;
    }

    protected void addMemory(int mem) {
        memory += mem;
    }

    protected void recalculateMemory() {
        int mem = DataUtils.PAGE_MEMORY;
        StorageDataType keyType = map.getKeyType();
        for (int i = 0; i < keys.length; i++) {
            mem += keyType.getMemory(keys[i]);
        }
        if (this.isLeaf()) {
            StorageDataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);
            }
        } else {
            mem += this.getRawChildPageCount() * DataUtils.PAGE_MEMORY_CHILD;
        }
        addMemory(mem - memory);
    }

    /**
     * Create a copy of this page.
     *
     * @return a page
     */
    public BTreePage copy() {
        BTreePage newPage = create(map, keys, values, children, totalCount, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.replicationHostIds = replicationHostIds;
        newPage.leafPageMovePlan = leafPageMovePlan;
        // mark the old as deleted
        removePage();
        return newPage;
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        long p = pos;
        if (p == 0) {
            removedInMemory = true;
        }
        map.storage.removePage(p, memory);
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            // TODO 消除这些难理解的规则
            // 不能直接使用getRawChildPageCount， RTreeMap这样的子类会返回getRawChildPageCount() - 1
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long pos = children[i].pos;
                    int type = DataUtils.getPageType(pos);
                    if (type == DataUtils.PAGE_TYPE_LEAF) {
                        int mem = DataUtils.getPageMaxLength(pos);
                        map.storage.removePage(pos, mem);
                    } else {
                        map.storage.readPage(pos).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof BTreePage) {
            if (pos != 0 && ((BTreePage) other).pos == pos) {
                return true;
            }
            return this == other;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    @Override
    public String toString() {
        if (DEBUG)
            return getPrettyPageInfo(false);

        StringBuilder buff = new StringBuilder();
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append("\n");
        if (pos != 0) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append("\n");
        }
        for (int i = 0; i <= keys.length; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[" + Long.toHexString(children[i].pos) + "] ");
            }
            if (i < keys.length) {
                buff.append(keys[i]);
                if (values != null) {
                    buff.append(':');
                    buff.append(values[i]);
                }
            }
        }
        return buff.toString();
    }

    String getPrettyPageInfo(boolean readOffLinePage) {
        StringBuilder buff = new StringBuilder();
        PrettyPageInfo info = new PrettyPageInfo();
        info.readOffLinePage = readOffLinePage;

        getPrettyPageInfoRecursive("", info);

        buff.append("PrettyPageInfo:").append('\n');
        buff.append("--------------").append('\n');
        buff.append("pageCount: ").append(info.pageCount).append('\n');
        buff.append("leafPageCount: ").append(info.leafPageCount).append('\n');
        buff.append("nodePageCount: ").append(info.nodePageCount).append('\n');
        buff.append("levelCount: ").append(info.levelCount).append('\n');
        buff.append('\n');
        buff.append("pages:").append('\n');
        buff.append("--------------------------------").append('\n');

        buff.append(info.buff).append('\n');

        return buff.toString();
    }

    private void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
        StringBuilder buff = info.buff;
        boolean readOffLinePage = info.readOffLinePage;
        info.pageCount++;
        if (isLeaf())
            info.leafPageCount++;
        else
            info.nodePageCount++;
        int levelCount = indent.length() / 2 + 1;
        if (levelCount > info.levelCount)
            info.levelCount = levelCount;

        buff.append(indent).append("type: ").append(isLeaf() ? "leaf" : "node").append('\n');
        buff.append(indent).append("pos: ").append(pos).append('\n');
        buff.append(indent).append("chunkId: ").append(DataUtils.getPageChunkId(pos)).append('\n');
        buff.append(indent).append("totalCount: ").append(totalCount).append('\n');
        buff.append(indent).append("memory: ").append(memory).append('\n');
        buff.append(indent).append("keyLength: ").append(keys.length).append('\n');

        if (keys.length > 0) {
            buff.append(indent).append("keys: ");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    buff.append(", ");
                buff.append(keys[i]);
            }
            buff.append('\n');

            if (isLeaf()) {
                buff.append(indent).append("values: ");
                for (int i = 0; i < keys.length; i++) {
                    if (i > 0)
                        buff.append(", ");
                    buff.append(values[i]);
                }
                buff.append('\n');
            } else {
                if (children != null) {
                    buff.append(indent).append("children: ").append(keys.length + 1).append('\n');
                    for (int i = 0; i <= keys.length; i++) {
                        buff.append('\n');
                        if (children[i].page != null) {
                            children[i].page.getPrettyPageInfoRecursive(indent + "  ", info);
                        } else {
                            if (readOffLinePage) {
                                map.storage.readPage(children[i].pos).getPrettyPageInfoRecursive(indent + "  ", info);
                            } else {
                                buff.append(indent).append("  ");
                                buff.append("*** off-line *** ").append(children[i]).append('\n');
                            }
                        }
                    }
                }
            }
        }
    }

    private static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }

    /**
     * Create a new, empty page.
     * 
     * @param map the map
     * @return the new page
     */
    static BTreePage createEmpty(BTreeMap<?, ?> map) {
        return create(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY, null, 0, DataUtils.PAGE_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     * 
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static BTreePage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, PageReference[] children,
            long totalCount, int memory) {
        BTreePage p = new BTreePage(map);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }

        return p;
    }

    /**
     * Read a page.
     * 
     * @param fileStorage the file storage
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static BTreePage read(FileStorage fileStorage, long pos, BTreeMap<?, ?> map, long filePos, long maxPos) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStorage.readFully(filePos, 128);
            maxLength = buff.getInt();
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ", length, filePos, maxPos);
        }
        buff = fileStorage.readFully(filePos, length);
        BTreePage p = new BTreePage(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength, DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF);
        return p;
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static class PageReference {

        /**
         * The page, if in memory, or null.
         */
        BTreePage page;

        /**
         * The position, if known, or 0.
         */
        final long pos;

        /**
         * The descendant count for this child page.
         */
        final long count;

        Object key;
        boolean last;

        public PageReference(BTreePage page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PageReference[ pos=" + pos + ", count=" + count + " ]";
        }
    }

    void transferTo(WritableByteChannel target, Object firstKey, Object lastKey) throws IOException {
        BTreePage firstPage = binarySearchLeafPage(this, firstKey);
        BTreePage lastPage = binarySearchLeafPage(this, lastKey);

        BTreeChunk chunk = map.storage.getChunk(firstPage.pos);
        long firstPos = firstPage.pos;
        long lastPos = lastPage.pos;

        map.storage.readPagePositions(chunk);
        ArrayList<long[]> pairs = new ArrayList<>();
        long pos;
        long pageLength;
        int index = 0;
        long[] pair;
        for (int i = 0, size = chunk.pagePositions.size(); i < size; i++) {
            pos = chunk.pagePositions.get(i);
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                if (firstPos <= pos && pos <= lastPos) {
                    pos = DataUtils.getPageOffset(pos);
                    pageLength = chunk.pageLengths.get(i);
                    if (index > 0) {
                        pair = pairs.get(index - 1);
                        if (pair[0] + pair[1] == pos) {
                            pair[1] += pageLength;
                            continue;
                        }
                    }
                    pair = new long[] { pos, pageLength };
                    pairs.add(pair);
                    index++;
                }
            }
        }

        long filePos;
        ByteBuffer buffer;
        for (long[] p : pairs) {
            filePos = BTreeStorage.getFilePos((int) p[0]);
            buffer = chunk.fileStorage.readFully(filePos, (int) p[1]);
            target.write(buffer);
        }
    }

    BTreePage binarySearchLeafPage(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            p = p.getChildPage(index);
            return binarySearchLeafPage(p, key);
        }
        if (index >= 0) {
            return p;
        }
        throw new AssertionError(); // 调用者已经确保key总是存在
    }

    void transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        ByteBuffer buff = ByteBuffer.allocateDirect((int) count);
        src.read(buff);
        buff.position((int) position);

        int pos = 0;
        while (buff.remaining() > 0) {
            int pageLength = buff.getInt();
            Object firstKey = map.getKeyType().read(buff);
            System.out.println("pageLength=" + pageLength + ", firstKey=" + firstKey);
            pos += pageLength;
            buff.position(pos);
        }
    }

    int replicatePage(DataBuffer buff, NetEndpoint localEndpoint) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = children != null ? DataUtils.PAGE_TYPE_NODE : DataUtils.PAGE_TYPE_LEAF;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        String hostAndPort = localEndpoint.getHostAndPort();
        map.storage.addHostIds(hostAndPort);
        ValueString.type.write(buff, hostAndPort);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            writeChildrenPositions(buff);
            for (int i = 0; i <= keyLength; i++) {
                buff.putVarLong(children[i].count); // count可能不大，所以用VarLong能节省一些空间
            }
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        if (type == DataUtils.PAGE_TYPE_LEAF) {
            map.getValueType().write(buff, values, keyLength);
            writeReplicationHostIds(buff);
        }
        BTreeStorage storage = map.getBTreeStorage();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = storage.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = storage.getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = storage.getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).put((byte) (type + compressType));
                    buff.position(compressStart).putVarInt(expLen - compLen).put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = 1;
        int check = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).putShort(checkPos, (short) check);
        return typePos + 1;
    }

    static BTreePage readPage(BTreeMap<?, ?> map, ByteBuffer buff) {
        Object[] keys = null;
        Object[] values = null;
        PageReference[] children = null;
        long totalCount = 0;
        List<String> replicationHostIds = null;

        int chunkId = 1;
        int maxLength = DataUtils.PAGE_LARGE;
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, maxLength,
                    pageLength);
        }
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }
        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        String remoteHostId = ValueString.type.read(buff);
        map.storage.addHostIds(remoteHostId);

        long remoteHostIdHashCode = 0;
        if (remoteHostId != null) {
            remoteHostIdHashCode = map.storage.getHostIdHashCode(remoteHostId);
        }
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[keyLength + 1];
            long[] p = new long[keyLength + 1];
            for (int i = 0; i <= keyLength; i++) {
                p[i] = buff.getLong();
                if (remoteHostId != null) {
                    p[i] = remoteHostIdHashCode;
                }
            }
            long total = 0;
            for (int i = 0; i <= keyLength; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) == DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getBTreeStorage().getCompressorHigh();
            } else {
                compressor = map.getBTreeStorage().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(), buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, keyLength);
        if (!node) {
            values = new Object[keyLength];
            map.getValueType().read(buff, values, keyLength);
            totalCount = keyLength;
            replicationHostIds = readReplicationHostIds(buff);
            map.storage.addHostIds(replicationHostIds);
        } else {
            for (int i = 0; i < keyLength; i++) {
                children[i].key = keys[i];
            }
            children[keyLength].key = keys[keyLength - 1];
            children[keyLength].last = true;
        }

        BTreePage p = BTreePage.create(map, keys, values, children, totalCount, 0);
        p.replicationHostIds = replicationHostIds;
        p.remoteHostId = remoteHostId;
        return p;
    }

    void readRemotePages() {
        for (int i = 0, length = children.length; i < length; i++) {
            final int index = i;
            Callable<BTreePage> task = new Callable<BTreePage>() {
                @Override
                public BTreePage call() throws Exception {
                    BTreePage p = getChildPage(index);
                    return p;
                }
            };
            AOStorageService.addTask(task);
        }
    }
}
