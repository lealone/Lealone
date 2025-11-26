/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.index.standard.IndexKey.SingleIndexKey;
import com.lealone.db.lock.Lock;
import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;
import com.lealone.storage.FormatVersion;

public abstract class IndexKeyType extends StandardDataType {

    public static IndexKeyType create(CompareMode compareMode, int[] sortTypes,
            StandardSecondaryIndex index) {
        if (sortTypes.length == 1) {
            return new SingleIndexKeyType(compareMode, sortTypes, index);
        } else {
            return new CompoundIndexKeyType(compareMode, sortTypes, index);
        }
    }

    protected final StandardSecondaryIndex index;

    /**
     * 用于优化唯一性检查，包括唯一约束、多字段primary key以及非byte/short/int/long类型的单字段primary key
     * 通过SecondaryIndex增加索引记录时不需要在执行addIfAbsent前后做唯一性检查
     */
    protected final boolean isUnique;

    public IndexKeyType(CompareMode compareMode, int[] sortTypes, StandardSecondaryIndex index) {
        super(compareMode, sortTypes);
        this.index = index;
        this.isUnique = index.getIndexType().isUnique();
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        long key;
        Value[] columns;
        Value v = DataBuffer.readValue(buff);
        if (v instanceof ValueArray)
            columns = ((ValueArray) v).getList();
        else
            columns = new Value[] { v };
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
        return IndexKey.create(key, columns);
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
            if (columns.length == 1)
                buff.writeValue(columns[0]);
            else
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
        return iKey.copy(iKey.getLockedValue(), null);
    }

    private static class SingleIndexKeyType extends IndexKeyType {

        public SingleIndexKeyType(CompareMode compareMode, int[] sortTypes,
                StandardSecondaryIndex index) {
            super(compareMode, sortTypes, index);
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
            Value ax = Lock.getLockedValue(aKey);
            Value bx = Lock.getLockedValue(bKey);
            if (bx == null) {
                bx = (Value) index.getDataMap().getOldValue(bKey);
                if (bx == null)
                    return 1;
            }
            int comp = compareValue(ax, bx, sortTypes[0]);
            if (comp == 0 && !isUnique) // 唯一索引key不需要比较最后的rowId
                return Long.compare(aKey.getKey(), bKey.getKey());
            else
                return Long.signum(comp);
        }

        @Override
        public int getMemory(Object obj) {
            SingleIndexKey k = (SingleIndexKey) obj;
            Value column = Lock.getLockedValue(k);
            return 32 + (column != null ? column.getMemory() : 0);
        }

        @Override
        public int getColumnsMemory(Object obj) {
            if (obj == null)
                return 0;
            else {
                Value column = null;
                if (obj instanceof Value[]) {
                    Value[] columns = (Value[]) obj;
                    if (columns.length > 0)
                        column = columns[0];
                } else if (obj instanceof Value) {
                    column = (Value) obj;
                }
                return column != null ? column.getMemory() : 0;
            }
        }
    }

    private static class CompoundIndexKeyType extends IndexKeyType {

        public CompoundIndexKeyType(CompareMode compareMode, int[] sortTypes,
                StandardSecondaryIndex index) {
            super(compareMode, sortTypes, index);
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
            if (comp == 0 && !isUnique) // 唯一索引key不需要比较最后的rowId
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
    }
}
