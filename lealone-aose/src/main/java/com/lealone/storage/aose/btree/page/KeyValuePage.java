/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataType;

public class KeyValuePage extends RowStorageLeafPage {

    // 内存占用40+48+56=144字节
    public static final int PAGE_MEMORY = 144;

    private Object[] values;

    public KeyValuePage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    protected int getPageType() {
        return 3;
    }

    @Override
    protected int getEmptyPageMemory() {
        return PAGE_MEMORY;
    }

    @Override
    protected Object[] getValues() {
        return values;
    }

    @Override
    protected void setValues(Object[] values) {
        this.values = values;
    }

    @Override
    public LeafPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] bKeys = splitKeys(a, b);

        Object[][] array = split(values, a, b);
        values = array[0];
        Object[] bValues = array[1];

        LeafPage newPage = create(map, bKeys, bValues, 0, getPageType());
        recalculateMemory();
        return newPage;
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        return copyAndInsertLeaf(index, key, value, values);
    }

    @Override
    public void remove(int index) {
        removeKey(index);
        values = removeValue(index, values);
        map.decrementSize(); // 递减全局计数器
    }

    @Override
    protected void readValues(ByteBuffer buff, int keyLength, int formatVersion) {
        values = new Object[keyLength];
        map.getValueType().read(buff, values, keyLength, formatVersion);
        setPageListener(map.getValueType(), values);
    }

    @Override
    protected void writeValues(DataBuffer buff, int keyLength, int formatVersion) {
        StorageDataType type = map.getValueType();
        for (int i = 0; i < keyLength; i++) {
            type.write(buff, values[i], formatVersion);
        }
    }
}
