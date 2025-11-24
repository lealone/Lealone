/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataType;

public class ColumnsPage extends ColumnStorageLeafPage {

    // 内存占用40+48+56=144字节
    public static final int PAGE_MEMORY = 144;

    public ColumnsPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    protected int getPageType() {
        return 2;
    }

    @Override
    protected int getEmptyPageMemory() {
        return PAGE_MEMORY;
    }

    @Override
    protected Object[] getValues() {
        return keys;
    }

    @Override
    public Object getSplitKey(int index) {
        return map.getKeyType().getSplitKey(getKey(index));
    }

    @Override
    protected int getKeyMemory(Object old) {
        // 也用值的类型来计算
        return map.getValueType().getMemory(old);
    }

    @Override
    protected StorageDataType getKeyTypeForRecalculateMemory() {
        // 也用值的类型来计算
        return map.getValueType();
    }

    @Override
    protected void readValues(ByteBuffer buff, int keyLength, int columnCount, int formatVersion) {
        StorageDataType valueType = map.getValueType();
        for (int row = 0; row < keyLength; row++) {
            keys[row] = valueType.readMeta(buff, keys[row], columnCount, formatVersion);
        }
        setPageListener(valueType, keys);
    }
}
