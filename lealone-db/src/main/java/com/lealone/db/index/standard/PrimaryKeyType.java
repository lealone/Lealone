/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;

import com.lealone.common.util.MathUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.row.Row;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;

//专门用于StandardPrimaryIndex，它的key是ValueLong且不为null
public class PrimaryKeyType extends StandardDataType {

    public PrimaryKeyType() {
        super(null, null, null);
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        return MathUtils.compareLong(((PrimaryKey) a).getKey(), ((PrimaryKey) b).getKey());
    }

    @Override
    public int getMemory(Object obj) {
        return ValueLong.type.getMemory(obj);
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        return DataBuffer.readValue(buff);
    }

    @Override
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        Value x;
        if (obj instanceof ValueLong)
            x = (ValueLong) obj;
        else
            x = ((Row) obj).getPrimaryKey();
        buff.writeValue(x);
    }

    @Override
    public Object getSplitKey(Object keyObj) {
        return ValueLong.get(((PrimaryKey) keyObj).getKey());
    }

    @Override
    public Object getAppendKey(long key, Object valueObj) {
        PrimaryKey pk = (PrimaryKey) valueObj;
        pk.setKey(key);
        return pk;
    }

    @Override
    public boolean isRowOnly() {
        return true;
    }
}
