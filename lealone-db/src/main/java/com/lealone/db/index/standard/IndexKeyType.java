/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;
import com.lealone.db.lock.Lock;
import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;
import com.lealone.storage.FormatVersion;

public class IndexKeyType extends StandardDataType {

    private final StandardSecondaryIndex index;

    public IndexKeyType(DataHandler handler, CompareMode compareMode, int[] sortTypes,
            StandardSecondaryIndex index) {
        super(handler, compareMode, sortTypes);
        this.index = index;
    }

    protected boolean isUniqueKey() {
        return false;
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
        IndexKey aKey = (IndexKey) a;
        IndexKey bKey = (IndexKey) b;
        Value[] ax = Lock.getLockedValue(aKey);
        Value[] bx = Lock.getLockedValue(bKey);
        if (bx == null) {
            bx = (Value[]) index.getDataMap().getOldValue(bKey);
            if (bx == null)
                return 1;
        }
        int comp = compareValues(ax, bx);
        if (comp == 0 && !isUniqueKey()) // 唯一索引key不需要比较最后的rowId
            return Long.compare(aKey.getKey(), bKey.getKey());
        else
            return Long.signum(comp);
    }

    @Override
    public int getMemory(Object obj) {
        IndexKey k = (IndexKey) obj;
        return 32 + RowType.getColumnsMemory(k);
    }

    @Override
    public int getColumnsMemory(Object obj) {
        return RowType.getColumnsMemory((Value[]) obj);
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        long key;
        ValueArray a = (ValueArray) DataBuffer.readValue(buff);
        Value[] columns = a.getList();
        if (columns.length == 0)
            columns = null;
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            if (columns != null) {
                key = columns[columns.length - 1].getLong();
                Value[] newColumns = new Value[columns.length - 1];
                System.arraycopy(columns, 0, newColumns, 0, newColumns.length);
                columns = newColumns;
            } else {
                key = 0;
            }
        } else {
            key = DataUtils.readVarLong(buff);
        }
        return new IndexKey(key, columns);
    }

    @Override
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        IndexKey iKey = (IndexKey) obj;
        Value[] columns = iKey.getColumns();
        if (columns == null) {
            columns = (Value[]) index.getDataMap().getOldValue(iKey);
            if (columns == null)
                columns = new Value[0];
        }
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            Value[] newColumns = new Value[columns.length + 1];
            System.arraycopy(columns, 0, newColumns, 0, columns.length);
            newColumns[columns.length] = ValueLong.get(iKey.getKey());
            buff.writeValue(ValueArray.get(newColumns));
        } else {
            buff.writeValue(ValueArray.get(columns));
            buff.putVarLong(iKey.getKey());
        }
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
        IndexKey iKey = (IndexKey) keyObj;
        return new IndexKey(iKey.getKey(), iKey.getColumns());
    }
}
