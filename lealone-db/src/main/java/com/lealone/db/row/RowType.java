/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.row;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;
import com.lealone.db.index.standard.StandardDataType;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.table.Column.EnumColumn;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;

public class RowType extends StandardDataType {

    final int columnCount;
    final EnumColumn[] enumColumns;

    public RowType(int[] sortTypes, int columnCount) {
        this(null, null, sortTypes, columnCount, null);
    }

    public RowType(DataHandler handler, CompareMode compareMode, int[] sortTypes, int columnCount,
            EnumColumn[] enumColumns) {
        super(handler, compareMode, sortTypes);
        this.columnCount = columnCount;
        this.enumColumns = enumColumns;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        if (aObj == null) {
            return -1;
        } else if (bObj == null) {
            return 1;
        }
        Row a = (Row) aObj;
        Row b = (Row) bObj;
        long comp = a.getVersion() - b.getVersion();
        if (comp == 0) {
            return compareValues(a.getColumns(), b.getColumns());
        }
        return Long.signum(comp);
    }

    @Override
    public int getMemory(Object obj) {
        Row r = (Row) obj;
        int memory = 24;
        if (r == null)
            return memory;
        Value[] columns = Lock.getLockedValue(r);
        for (int i = 0, len = columns.length; i < len; i++) {
            Value c = columns[i];
            if (c == null)
                memory += 4;
            else
                memory += c.getMemory();
        }
        return memory;
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            obj[i] = merge(obj[i], read(buff));
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        int version = DataUtils.readVarInt(buff);
        ValueArray a = (ValueArray) DataBuffer.readValue(buff);
        if (enumColumns != null)
            setEnumColumns(a);
        return new Row(version, a.getList());
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        Row r = (Row) obj;
        buff.putVarInt(r.getVersion());
        buff.writeValue(ValueArray.get(r.getColumns()));
    }

    @Override
    public void write(DataBuffer buff, Object obj, Lockable lockable) {
        Row r = (Row) lockable;
        buff.putVarInt(r.getVersion());
        buff.writeValue(ValueArray.get((Value[]) obj));
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj) {
        Row r = (Row) obj;
        buff.putVarInt(r.getVersion());
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        int version = DataUtils.readVarInt(buff);
        Value[] columns = new Value[columnCount];
        return new Row(version, columns);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        Row r = (Row) obj;
        Value[] columns = r.getColumns();
        if (columnIndex >= 0 && columnIndex < columns.length)
            buff.writeValue(columns[columnIndex]);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        Row r = (Row) obj;
        Value[] columns = r.getColumns();
        if (columnIndex >= 0 && columnIndex < columns.length) {
            Value value = DataBuffer.readValue(buff);
            columns[columnIndex] = value;
            if (enumColumns != null)
                setEnumColumn(value, columnIndex);
        }
    }

    @Override
    public void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        if (columnIndexes != null) {
            Row oldRow = (Row) oldObj;
            Row newRow = (Row) newObj;
            Value[] oldColumns = oldRow.getColumns();
            Value[] newColumns = newRow.getColumns();
            for (int i : columnIndexes) {
                oldColumns[i] = newColumns[i];
            }
        }
    }

    @Override
    public ValueArray getColumns(Object obj) {
        return ValueArray.get(((Row) obj).getColumns());
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        Row r = (Row) obj;
        Value[] columns = r.getColumns();
        if (columnIndex >= 0 && columnIndex < columns.length) {
            return columns[columnIndex].getMemory();
        } else {
            return 0;
        }
    }

    private void setEnumColumn(Value value, int columnIndex) {
        if (enumColumns[columnIndex] != null)
            enumColumns[columnIndex].setLabel(value);
    }

    private void setEnumColumns(ValueArray a) {
        for (int i = 0, len = a.getList().length; i < len; i++) {
            setEnumColumn(a.getValue(i), i);
        }
    }

    @Override
    public Object merge(Object fromObj, Object toObj) {
        Row row = (Row) toObj;
        if (fromObj != null)
            row.setKey(((ValueLong) fromObj).getLong());
        return row;
    }
}
