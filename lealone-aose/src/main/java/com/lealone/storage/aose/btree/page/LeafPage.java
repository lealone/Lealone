/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import com.lealone.common.util.DataUtils;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataType;

public abstract class LeafPage extends LocalPage {

    protected LeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    protected abstract int getPageType();

    protected void setKeys(Object[] keys) {
        this.keys = keys;
    }

    protected void setValues(Object[] values) {
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return keys == null || keys.length == 0;
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = getValues()[index];
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        getValues()[index] = value;
        setPageListener(valueType, value);
        return old;
    }

    @Override
    public void remove(int index) {
        removeKey(index);
        map.decrementSize(); // 递减全局计数器
    }

    protected Object[] removeValue(int index, Object[] values) {
        int length = values.length;
        Object old = values[index];
        addMemory(-map.getValueType().getMemory(old));
        Object[] newValues = new Object[length - 1];
        DataUtils.copyExcept(values, newValues, length, index);
        return newValues;
    }

    @Override
    public LeafPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
        LeafPage newPage = create(map, splitKeys(at), 0, getPageType());
        recalculateMemory();
        return newPage;
    }

    protected Object[] splitKeys(int at) {
        int a = at, b = keys.length - a;
        return splitKeys(a, b);
    }

    protected Object[] splitKeys(int a, int b) {
        Object[][] array = split(keys, a, b);
        keys = array[0];
        return array[1];
    }

    protected static Object[][] split(Object[] objs, int a, int b) {
        Object[] aObjs = new Object[a];
        Object[] bObjs = new Object[b];
        System.arraycopy(objs, 0, aObjs, 0, a);
        System.arraycopy(objs, a, bObjs, 0, b);
        return new Object[][] { aObjs, bObjs };
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        newKeys[index] = value; // 只有keys没有values的page只存放value
        LeafPage p = copyLeaf(newKeys, null);
        StorageDataType valueType = map.getValueType();
        p.addMemory(valueType.getMemory(value));
        map.incrementSize();// 累加全局计数器
        setPageListener(valueType, value);
        return p;
    }

    protected Page copyAndInsertLeaf(int index, Object key, Object value, Object[] values) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        newKeys[index] = key;
        newValues[index] = value;
        LeafPage p = copyLeaf(newKeys, newValues);
        StorageDataType valueType = map.getValueType();
        p.addMemory(map.getKeyType().getMemory(key) + valueType.getMemory(value));
        map.incrementSize();// 累加全局计数器
        setPageListener(valueType, value);
        return p;
    }

    protected void setPageListener(StorageDataType type, Object value) {
        if (type.isLockable()) {
            Lockable lockable = (Lockable) value;
            Lock lock = lockable.getLock();
            if (lock != null)
                lock.setPageListener(getRef().getPageListener());
            else
                lockable.setLock(getRef().getLock());
        }
    }

    protected void setPageListener(StorageDataType type, Object[] objs) {
        if (type.isLockable()) {
            PageReference ref = getRef();
            if (ref == null) // 执行ChunkCompactor.rewrite时为null
                return;
            Lock lock = ref.getLock();
            for (Object obj : objs) {
                ((Lockable) obj).setLock(lock);
            }
        }
    }

    protected boolean isLocked(Object obj) {
        return !((Lockable) obj).isNoneLock();
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        if (getPageType() >= 3) {
            Object[] values = getValues();
            StorageDataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);
            }
        }
        addMemory(mem - memory, false);
    }

    @Override
    public LeafPage copy() {
        if (getPageType() < 3)
            return copyLeaf(keys, null);
        else
            return copyLeaf(keys, getValues());
    }

    protected LeafPage copyLeaf(Object[] keys, Object[] values) {
        LeafPage newPage = create(map, keys, values, getMemory(), getPageType());
        super.copy(newPage);
        return newPage;
    }

    public static LeafPage createEmpty(BTreeMap<?, ?> map, boolean addToUsedMemory) {
        int pageType;
        if (map.getKeyType().isKeyOnly()) {
            pageType = 0;
        } else if (map.getValueType().isRowOnly()) {
            if (map.getPageStorageMode() == PageStorageMode.ROW_STORAGE)
                pageType = 1;
            else
                pageType = 2;
        } else {
            if (map.getPageStorageMode() == PageStorageMode.ROW_STORAGE)
                pageType = 3;
            else
                pageType = 4;
        }
        LeafPage p = create(map, pageType);
        int memory = p.getEmptyPageMemory();
        if (addToUsedMemory)
            map.getBTreeStorage().getBTreeGC().addUsedMemory(memory);
        initPage(p, new Object[0], new Object[0], memory);
        return p;
    }

    public static LeafPage create(BTreeMap<?, ?> map, Object[] keys, int memory, int pageType) {
        return create(map, keys, null, memory, pageType);
    }

    public static LeafPage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, int memory,
            int pageType) {
        // the position is 0
        LeafPage p = create(map, pageType);
        initPage(p, keys, values, memory);
        return p;
    }

    private static void initPage(LeafPage p, Object[] keys, Object[] values, int memory) {
        p.setKeys(keys);
        p.setValues(values);
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory, false);
        }
    }

    private static LeafPage create(BTreeMap<?, ?> map, int pageType) {
        switch (pageType) {
        case 0:
            return new KeyPage(map);
        case 1:
            return new RowPage(map);
        case 2:
            return new ColumnsPage(map);
        case 3:
            return new KeyValuePage(map);
        default:
            return new KeyColumnsPage(map);
        }
    }
}
