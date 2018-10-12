/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;

import org.lealone.common.compress.Compressor;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
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
public abstract class BTreeLocalPage extends BTreePage {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;
    public static final boolean DEBUG = true;

    /**
     * The total entry count of this page and all children.
     */
    protected long totalCount;

    /**
     * The last result of a find operation is cached.
     */
    protected int cachedCompare;

    /**
     * The estimated memory used.
     */
    protected int memory;

    /**
     * The keys.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    protected Object[] keys;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    protected volatile boolean removedInMemory;

    protected BTreeLocalPage(BTreeMap<?, ?> map) {
        super(map);
    }

    /**
    * Get the key at the given index.
    * 
    * @param index the index
    * @return the key
    */
    @Override
    public Object getKey(int index) {
        return keys[index];
    }

    /**
     * Get the number of keys in this page.
     * 
     * @return the number of keys
     */
    @Override
    public int getKeyCount() {
        return keys.length;
    }

    @Override
    Object getLastKey() {
        if (keys == null || keys.length == 0)
            return null;
        else
            return keys[keys.length - 1];
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
    @Override
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

    @Override
    boolean needSplit() {
        return memory > map.btreeStorage.getPageSplitSize() && keys.length > 1;
    }

    /**
     * Replace the key at an index in this page.
     * 
     * @param index the index
     * @param key the new key
     */
    @Override
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

    @Override
    public void remove(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        Object old = keys[keyIndex];
        addMemory(-map.getKeyType().getMemory(old));
        Object[] newKeys = new Object[keyLength - 1];
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;
    }

    protected abstract void recalculateMemory();

    protected int recalculateKeysMemory() {
        int mem = PageUtils.PAGE_MEMORY;
        StorageDataType keyType = map.getKeyType();
        for (int i = 0, len = keys.length; i < len; i++) {
            mem += keyType.getMemory(keys[i]);
        }
        return mem;
    }

    @Override
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

    /**
     * Remove the page.
     */
    @Override
    public void removePage() {
        long p = pos;
        if (p == 0) {
            removedInMemory = true;
        }
        map.btreeStorage.removePage(p, memory);
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

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof BTreeLocalPage) {
            if (pos != 0 && ((BTreeLocalPage) other).pos == pos) {
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
            int chunkId = PageUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append("\n");
        }
        toString0(buff);
        return buff.toString();
    }

    protected abstract void toString0(StringBuilder buff);

    @Override
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

    @Override
    void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
        StringBuilder buff = info.buff;
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
        buff.append(indent).append("chunkId: ").append(PageUtils.getPageChunkId(pos)).append('\n');
        buff.append(indent).append("totalCount: ").append(totalCount).append('\n');
        buff.append(indent).append("memory: ").append(memory).append('\n');
        buff.append(indent).append("keyLength: ").append(keys.length).append('\n');

        if (keys.length > 0) {
            buff.append(indent).append("keys: ");
            for (int i = 0, len = keys.length; i < len; i++) {
                if (i > 0)
                    buff.append(", ");
                buff.append(keys[i]);
            }
            buff.append('\n');
            getPrettyPageInfoRecursive(buff, indent, info);
        }
    }

    protected abstract void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info);
}
