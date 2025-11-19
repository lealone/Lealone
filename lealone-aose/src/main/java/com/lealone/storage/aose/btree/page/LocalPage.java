/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import com.lealone.common.util.DataUtils;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataType;

public abstract class LocalPage extends Page {

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

    protected LocalPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object getKey(int index) {
        return keys[index];
    }

    @Override
    public int getKeyCount() {
        return keys.length;
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
    public boolean needSplit() {
        return memory > map.getBTreeStorage().getPageSize() && keys.length > 1;
    }

    protected void removeKey(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        Object old = keys[keyIndex];
        addMemory(-getKeyMemory(old));
        Object[] newKeys = new Object[keyLength - 1];
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;
    }

    protected int getKeyMemory(Object old) {
        return map.getKeyType().getMemory(old);
    }

    protected StorageDataType getKeyTypeForRecalculateMemory() {
        return map.getKeyType();
    }

    protected abstract int getEmptyPageMemory();

    protected abstract void recalculateMemory();

    protected int recalculateKeysMemory() {
        int mem = getEmptyPageMemory();
        StorageDataType keyType = getKeyTypeForRecalculateMemory();
        for (int i = 0, len = keys.length; i < len; i++) {
            // 忽略数组元素占用的字节，简化实现
            // mem += 4; // 数组元素占4个字节
            mem += keyType.getMemory(keys[i]);
        }
        return mem;
    }

    @Override
    public int getMemory() {
        return memory;
    }

    @Override
    public void addMemory(int mem) {
        addMemory(mem, true);
    }

    protected void addMemory(int mem, boolean addToUsedMemory) {
        memory += mem;
        if (addToUsedMemory)
            map.getBTreeStorage().getBTreeGC().addUsedMemory(mem);
    }

    protected void copy(LocalPage newPage) {
        newPage.cachedCompare = cachedCompare;
        newPage.setRef(getRef());
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof LocalPage) {
            long pos = getPos();
            if (pos != 0 && ((LocalPage) other).getPos() == pos) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
        // long pos = getPos();
        // return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    @Override
    public String toString() {
        return PrettyPagePrinter.getPrettyPageInfo(this, false);
    }
}
