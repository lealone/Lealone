/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.type.StorageDataType;

public abstract class LocalPage extends Page {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

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

    protected LocalPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object[] getKeys() {
        return keys;
    }

    @Override
    public Object getKey(int index) {
        return keys[index];
    }

    @Override
    public int getKeyCount() {
        return keys.length;
    }

    @Override
    public Object getLastKey() {
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
    }

    @Override
    boolean needSplit() {
        return memory > map.getBTreeStorage().getPageSplitSize() && keys.length > 1;
    }

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
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                        "Memory calculation error");
            }
        }
        return memory;
    }

    protected void addMemory(int mem) {
        memory += mem;
    }

    @Override
    public void removePage() {
        if (pos == 0) {
            removedInMemory = true;
        }
        map.getBTreeStorage().removePage(pos, memory);
    }

    protected void removeIfInMemory() {
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.getBTreeStorage().removePage(pos, memory);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof LocalPage) {
            if (pos != 0 && ((LocalPage) other).pos == pos) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    @Override
    public String toString() {
        return getPrettyPageInfo(false);
    }

    ////////////////////// 打印出漂亮的由page组成的btree ////////////////////////////////

    @Override
    public String getPrettyPageInfo(boolean readOffLinePage) {
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
        // buff.append(indent).append("totalCount: ").append(getTotalCount()).append('\n');
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

    protected abstract void getPrettyPageInfoRecursive(StringBuilder buff, String indent,
            PrettyPageInfo info);
}
