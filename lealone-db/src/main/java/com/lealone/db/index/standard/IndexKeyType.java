/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;
import com.lealone.db.lock.Lock;
import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;

public class IndexKeyType extends StandardDataType {

    private final StandardSecondaryIndex index;

    public IndexKeyType(DataHandler handler, CompareMode compareMode, int[] sortTypes,
            StandardSecondaryIndex index) {
        super(handler, compareMode, sortTypes);
        this.index = index;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        IndexKey bKey = (IndexKey) b;
        Value[] ax = Lock.getLockedValue((IndexKey) a);
        Value[] bx = Lock.getLockedValue(bKey);
        if (bx == null) {
            bx = (Value[]) index.getDataMap().getOldValue(bKey);
            if (bx == null)
                return 1;
        }
        return compareValues(ax, bx);
    }

    @Override
    public int getMemory(Object obj) {
        IndexKey k = (IndexKey) obj;
        return 24 + RowType.getColumnsMemory(k);
    }

    @Override
    public int getColumnsMemory(Object obj) {
        return RowType.getColumnsMemory((Value[]) obj);
    }

    @Override
    public Object read(ByteBuffer buff) {
        ValueArray a = (ValueArray) DataBuffer.readValue(buff);
        Value[] columns = a.getList();
        if (columns.length == 0)
            columns = null;
        return new IndexKey(columns);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        IndexKey k = (IndexKey) obj;
        Value[] columns = k.columns;
        if (columns == null) {
            columns = (Value[]) index.getDataMap().getOldValue(k);
            if (columns == null)
                columns = new Value[0];
        }
        buff.writeValue(ValueArray.get(columns));
    }

    @Override
    public Object convertToIndexKey(Object key, Object value) {
        return index.convertToKey((Row) value);
    }

    @Override
    public boolean isLockable() {
        return true;
    }

    @Override
    public boolean isKeyOnly() {
        return true;
    }

    @Override
    public Object getSplitKey(Object keyObj) {
        return new IndexKey(((IndexKey) keyObj).columns);
    }
}
