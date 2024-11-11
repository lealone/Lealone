/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;

public class RowPage extends RowStorageLeafPage {

    public RowPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    protected int getPageType() {
        return 1;
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
    protected void readValues(ByteBuffer buff, int keyLength) {
        map.getValueType().read(buff, keys, keyLength);
    }

    @Override
    protected void writeValues(DataBuffer buff, int keyLength) {
        map.getValueType().write(buff, keys, keyLength);
    }
}
