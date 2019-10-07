/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.aose.btree.PageOperations.TmpNodePage;
import org.lealone.storage.fs.FileStorage;

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

    public static class DynamicInfo {
        public final State state;
        public final BTreePage redirect;

        public DynamicInfo() {
            this(State.NORMAL, null);
        }

        public DynamicInfo(State state, BTreePage redirect) {
            this.state = state;
            this.redirect = redirect;
        }

        public static DynamicInfo redirectTo(BTreePage p) {
            return new DynamicInfo(State.NORMAL, p);
        }
    }

    public static enum State {
        NORMAL,
        SPLITTING,
        SPLITTED;

        BTreePage redirect;

        public BTreePage getRedirect() {
            return redirect;
        }

        public void setRedirect(BTreePage redirect) {
            this.redirect = redirect;
        }
    }

    /**
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static final AtomicLong ids = new AtomicLong(0);

    protected final BTreeMap<?, ?> map;
    protected long pos;
    protected org.lealone.storage.PageOperationHandler handler;
    // private final int handlerId;

    // BTreePage parent;

    volatile DynamicInfo dynamicInfo = new DynamicInfo();
    // AtomicLong counter = new AtomicLong();
    int size;
    final long id;
    final ConcurrentLinkedQueue<PageOperation> tasks = new ConcurrentLinkedQueue<>();

    private boolean splitEnabled = true;

    protected BTreePage(BTreeMap<?, ?> map) {
        this.map = map;
        id = ids.incrementAndGet();
        if (isNode())
            handler = map.pohFactory.getNodePageOperationHandler();
        else if (isLeaf())
            handler = map.pohFactory.getPageOperationHandler();
        // if (handler != null)
        // handler.addQueue(id, tasks);
    }

    public boolean isSplitEnabled() {
        return splitEnabled;
    }

    public void enableSplit() {
        splitEnabled = true;
    }

    public void disableSplit() {
        splitEnabled = false;
    }

    public void redirectTo(BTreePage p) {
        dynamicInfo = DynamicInfo.redirectTo(p);
    }

    public void setHandler(PageOperationHandler handler) {
        this.handler = handler;
    }

    public PageOperationHandler getHandler() {
        return handler; // PageOperationHandler.getHandler(0); // throw ie();
    }

    public void addTask(PageOperation task) {
        if (handler != null) {
            // tasks.add(task);
            // handler.addQueue(id, tasks);
            // handler.wakeUp();
            handler.handlePageOperation(task);
        }
    }

    public void updateCount(long delta) {
    }

    /**
     * Get the position of the page
     * 
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    private static RuntimeException ie() {
        return DbException.throwInternalError();
    }

    /**
    * Get the key at the given index.
    * 
    * @param index the index
    * @return the key
    */
    public Object getKey(int index) {
        throw ie();
    }

    /**
     * Get the value at the given index.
     * 
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        throw ie();
    }

    public Object getValue(int index, int columnIndex) {
        throw ie();
    }

    public Object getValue(int index, int[] columnIndexes) {
        throw ie();
    }

    public Object getValue(int index, boolean allColumns) {
        throw ie();
    }

    public boolean isEmpty() {
        return getTotalCount() <= 0;
    }

    public boolean isNotEmpty() {
        return getTotalCount() > 0;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     * 
     * @return the number of key-value pairs
     */
    public long getTotalCount() {
        return 0;
    }

    public AtomicLong getCounter() {
        return null;
    }

    /**
     * Get the number of keys in this page.
     * 
     * @return the number of keys
     */
    public int getKeyCount() {
        throw ie();
    }

    /**
     * Get the child page at the given index.
     * 
     * @param index the index
     * @return the child page
     */
    public BTreePage getChildPage(int index) {
        throw ie();
    }

    PageReference getChildPageReference(int index) {
        throw ie();
    }

    /**
     * Check whether this is a leaf page.
     * 
     * @return true if it is a leaf page
     */
    public boolean isLeaf() {
        return false;
    }

    /**
     * Check whether this is a node page.
     * 
     * @return true if it is a node page
     */
    public boolean isNode() {
        return false;
    }

    /**
     * Check whether this is a remote page.
     * 
     * @return true if it is a remote page
     */
    public boolean isRemote() {
        return false;
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
        throw ie();
    }

    boolean needSplit() {
        throw ie();
    }

    /**
     * Split the page. This modifies the current page.
     * 
     * @param at the split index
     * @return the page with the entries after the split index
     */
    BTreePage split(int at) {
        throw ie();
    }

    /**
     * Replace the key at an index in this page.
     * 
     * @param index the index
     * @param key the new key
     */
    public void setKey(int index, Object key) {
        throw ie();
    }

    /**
     * Replace the value at an index in this page.
     * 
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public Object setValue(int index, Object value) {
        throw ie();
    }

    /**
     * Replace the child page.
     * 
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, BTreePage c) {
        throw ie();
    }

    public void setChild(int index, BTreePage c, boolean updateTotalCount) {
        throw ie();
    }

    public void setChild(int index, PageReference ref) {
        throw ie();
    }

    public void setChild(long childCount) {
        throw ie();
    }

    void setAndInsertChild(int index, TmpNodePage tmpNodePage) {
        throw ie();
    }

    /**
     * Insert a key-value pair into this leaf.
     * 
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        throw ie();
    }

    /**
     * Insert a child page into this node.
     * 
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, BTreePage childPage) {
        throw ie();
    }

    /**
     * Remove the key and value (or child) at the given index.
     * 
     * @param index the index
     */
    public void remove(int index) {
        throw ie();
    }

    /**
     * Read the page from the buffer.
     * 
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        read(buff, chunkId, offset, maxLength, false);
    }

    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        throw ie();
    }

    /**
    * Store the page and update the position.
    *
    * @param chunk the chunk
    * @param buff the target buffer
    * @return the position of the buffer just after the type
    */
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        throw ie();
    }

    void writeLeaf(DataBuffer buff, boolean remote) {
        throw ie();
    }

    static BTreePage readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        int type = page.get();
        if (type == PageUtils.PAGE_TYPE_LEAF)
            return BTreeLeafPage.readLeafPage(map, page);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            return BTreeRemotePage.readLeafPage(map, page);
        else
            throw DbException.throwInternalError("type: " + type);
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
        throw ie();
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
    }

    public int getRawChildPageCount() {
        return 0;
    }

    public int getMemory() {
        return 0;
    }

    /**
     * Create a copy of this page.
     *
     * @return a page
     */
    public BTreePage copy() {
        throw ie();
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        throw ie();
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        throw ie();
    }

    String getPrettyPageInfo(boolean readOffLinePage) {
        throw ie();
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
        int maxLength = PageUtils.getPageMaxLength(pos);
        ByteBuffer buff = readPageBuff(fileStorage, maxLength, filePos, maxPos);
        int type = PageUtils.getPageType(pos);
        BTreePage p = create(map, type);
        p.pos = pos;
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    static ByteBuffer readPageBuff(FileStorage fileStorage, int maxLength, long filePos, long maxPos) {
        ByteBuffer buff;
        if (maxLength == PageUtils.PAGE_LARGE) {
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
        return buff;
    }

    private static BTreePage create(BTreeMap<?, ?> map, int type) {
        BTreePage p;
        if (type == PageUtils.PAGE_TYPE_LEAF)
            p = new BTreeLeafPage(map);
        else if (type == PageUtils.PAGE_TYPE_NODE)
            p = new BTreeNodePage(map);
        else if (type == PageUtils.PAGE_TYPE_COLUMN)
            p = new BTreeColumnPage(map);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            p = new BTreeRemotePage(map);
        else
            throw DbException.throwInternalError("type: " + type);
        return p;
    }

    @Deprecated
    void transferTo(WritableByteChannel target, Object firstKey, Object lastKey) throws IOException {
        BTreePage firstPage = binarySearchLeafPage(firstKey);
        BTreePage lastPage = binarySearchLeafPage(lastKey);

        BTreeChunk chunk = map.btreeStorage.getChunk(firstPage.pos);
        long firstPos = firstPage.pos;
        long lastPos = lastPage.pos;

        map.btreeStorage.readPagePositions(chunk);
        ArrayList<long[]> pairs = new ArrayList<>();
        long pos;
        long pageLength;
        int index = 0;
        long[] pair;
        for (int i = 0, size = chunk.pagePositions.size(); i < size; i++) {
            pos = chunk.pagePositions.get(i);
            if (PageUtils.getPageType(pos) == PageUtils.PAGE_TYPE_LEAF) {
                if (firstPos <= pos && pos <= lastPos) {
                    pos = PageUtils.getPageOffset(pos);
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

    // 返回key所在的leaf page
    BTreePage binarySearchLeafPage(Object key) {
        BTreePage p = this;
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                // 如果找不到，是返回null还是throw new AssertionError()，由调用者确保key总是存在
                return index >= 0 ? p : null;
            } else {
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                p = p.getChildPage(index);
            }
        }
    }

    // 只找到key对应的LeafPage就行了，不关心key是否存在
    public BTreePage gotoLeafPage(Object key) {
        BTreePage p = this;
        while (p.isNode()) {
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            p = p.getChildPage(index);
        }
        return p;
    }

    @Deprecated
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

    void readRemotePages() {
        throw ie();
    }

    void readRemotePagesRecursive() {
        throw ie();
    }

    void moveAllLocalLeafPages(String[] oldEndpoints, String[] newEndpoints) {
        throw ie();
    }

    void replicatePage(DataBuffer buff, NetEndpoint localEndpoint) {
        throw ie();
    }

    static BTreePage readReplicatedPage(BTreeMap<?, ?> map, ByteBuffer buff) {
        int type = buff.get();
        BTreePage p = create(map, type);
        int chunkId = 0;
        int offset = buff.position();
        p.read(buff, chunkId, offset, buff.limit());
        return p;
    }

    boolean isRemoteChildPage(int index) {
        return false;
    }

    boolean isNodeChildPage(int index) {
        return false;
    }

    boolean isLeafChildPage(int index) {
        return false;
    }

    Object getLastKey() {
        throw ie();
    }

    // test only
    public PageReference[] getChildren() {
        throw ie();
    }

    public void setReplicationHostIds(List<String> replicationHostIds) {
    }

    public List<String> getReplicationHostIds() {
        return null;
    }

    static void writeReplicationHostIds(List<String> replicationHostIds, DataBuffer buff) {
        if (replicationHostIds == null || replicationHostIds.isEmpty())
            buff.putInt(0);
        else {
            buff.putInt(replicationHostIds.size());
            for (String id : replicationHostIds) {
                ValueString.type.write(buff, id);
            }
        }
    }

    static List<String> readReplicationHostIds(ByteBuffer buff) {
        int length = buff.getInt();
        List<String> replicationHostIds = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
            replicationHostIds.add(ValueString.type.read(buff));

        if (replicationHostIds.isEmpty())
            replicationHostIds = null;

        return replicationHostIds;
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
    public static BTreeLocalPage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, PageReference[] children,
            AtomicLong totalCount, int memory) {
        if (children != null)
            return BTreeNodePage.create(map, keys, children, totalCount, memory);
        else
            return BTreeLeafPage.create(map, keys, values, totalCount, memory);
    }

    static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }

    void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
    }

    public LeafPageMovePlan getLeafPageMovePlan() {
        return null;
    }

    public void setLeafPageMovePlan(LeafPageMovePlan leafPageMovePlan) {
    }

    static void readCheckValue(ByteBuffer buff, int chunkId, int offset, int pageLength, boolean disableCheck) {
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (!disableCheck && check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }
    }

    static void writeCheckValue(DataBuffer buff, int chunkId, int start, int pageLength, int checkPos) {
        int check = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putShort(checkPos, (short) check);
    }

    static void checkPageLength(int chunkId, int pageLength, int maxLength) {
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, maxLength,
                    pageLength);
        }
    }

    void compressPage(DataBuffer buff, int compressStart, int type, int typePos) {
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            BTreeStorage storage = map.getBTreeStorage();
            int compressionLevel = storage.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = storage.getCompressorFast();
                    compressType = PageUtils.PAGE_COMPRESSED;
                } else {
                    compressor = storage.getCompressorHigh();
                    compressType = PageUtils.PAGE_COMPRESSED_HIGH;
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
    }

    ByteBuffer expandPage(ByteBuffer buff, int type, int start, int pageLength) {
        boolean compressed = (type & PageUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & PageUtils.PAGE_COMPRESSED_HIGH) == PageUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getBTreeStorage().getCompressorHigh();
            } else {
                compressor = map.getBTreeStorage().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            ByteBuffer newBuff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, newBuff.array(), newBuff.arrayOffset(), l);
            return newBuff;
        }
        return buff;
    }

    void updateChunkAndCachePage(BTreeChunk chunk, int start, int pageLength, int type) {
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = PageUtils.getPagePos(chunk.id, start, pageLength, type);
        chunk.pagePositions.add(pos);
        chunk.pageLengths.add(pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        map.getBTreeStorage().cachePage(pos, this, getMemory());

        if (chunk.sumOfPageLength > BTreeChunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", BTreeChunk.MAX_SIZE, chunk.sumOfPageLength);
    }

    public Object[] getKeys() {
        throw ie();
    }

    public Object[] getValues() {
        throw ie();
    }
}
