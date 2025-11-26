/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.row;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.index.standard.StandardDataType;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.table.Column.EnumColumn;
import com.lealone.db.table.StandardTable;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.storage.FormatVersion;

public class RowType extends StandardDataType {

    private final int columnCount;
    private final EnumColumn[] enumColumns;
    private final StandardTable table;

    public RowType(int[] sortTypes, int columnCount) {
        this(null, sortTypes, columnCount, null);
    }

    public RowType(CompareMode compareMode, int[] sortTypes, int columnCount, StandardTable table) {
        super(compareMode, sortTypes);
        this.columnCount = columnCount;
        if (table != null) {
            this.enumColumns = table.getEnumColumns();
            this.table = table;
        } else {
            this.enumColumns = null;
            this.table = null;
        }
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
        long comp = a.getMetaVersion() - b.getMetaVersion();
        if (comp == 0) {
            return compareValues(a.getColumns(), b.getColumns());
        }
        return Long.signum(comp);
    }

    @Override
    public int getMemory(Object obj) {
        Row r = (Row) obj;
        return 32 + getColumnsMemory(r);
    }

    @Override
    public int getColumnsMemory(Object obj) {
        return getColumnsMemory((Value[]) obj);
    }

    public static int getColumnsMemory(Value[] columns) {
        int memory = 0;
        if (columns == null)
            return memory;
        // 16是数组header的长度
        memory += 16 + columns.length * 4;
        for (int i = 0, len = columns.length; i < len; i++) {
            Value c = columns[i];
            if (c != null)
                memory += c.getMemory();
        }
        return memory;
    }

    public static int getColumnsMemory(Lockable lockable) {
        Value[] columns = Lock.getLockedValue(lockable);
        return getColumnsMemory(columns);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, int formatVersion) {
        for (int i = 0; i < len; i++) {
            obj[i] = merge(obj[i], read(buff, formatVersion));
        }
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion))
            DataUtils.readVarInt(buff); // version
        ValueArray a = (ValueArray) DataBuffer.readValue(buff);
        if (a == null || a.getList().length == 0)
            return new Row(null);
        if (enumColumns != null)
            setEnumColumns(a);
        return new Row(a.getList());
    }

    @Override
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        Row r = (Row) obj;
        Value[] columns = r.getColumns();
        write(buff, r, columns, formatVersion);
    }

    @Override
    public void write(DataBuffer buff, Lockable lockable, Object lockedValue, int formatVersion) {
        write(buff, (Row) lockable, (Value[]) lockedValue, formatVersion);
    }

    private void write(DataBuffer buff, Row r, Value[] columns, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion))
            buff.putVarInt(r.getMetaVersion());
        if (columns == null)
            columns = new Value[0];
        buff.writeValue(ValueArray.get(columns));
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            Row r = (Row) obj;
            buff.putVarInt(r.getMetaVersion());
        }
    }

    @Override
    public Object readMeta(ByteBuffer buff, Object obj, int columnCount, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion))
            DataUtils.readVarInt(buff);
        Value[] columns = new Value[columnCount];
        Row row = new Row(columns);
        if (rowOnly) {
            return merge(obj, row);
        } else {
            return row;
        }
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex, int formatVersion) {
        Row r = (Row) obj;
        Value[] columns = r.getColumns();
        if (columnIndex >= 0 && columnIndex < columns.length)
            buff.writeValue(columns[columnIndex]);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex, int formatVersion) {
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
        Row row = toObj != null ? (Row) toObj : new Row(null);
        if (fromObj instanceof PrimaryKey)
            row.setKey(((PrimaryKey) fromObj).getKey());
        else if (fromObj instanceof Value)
            row.setKey(((Value) fromObj).getLong());
        else if (fromObj instanceof Number)
            row.setKey(((Number) fromObj).longValue());
        return row;
    }

    @Override
    public boolean isLockable() {
        return true;
    }

    private boolean rowOnly;

    @Override
    public boolean isRowOnly() {
        return rowOnly;
    }

    @Override
    public void setRowOnly(boolean rowOnly) {
        this.rowOnly = rowOnly;
    }

    @Override
    public int getMetaVersion() {
        return table != null ? table.getVersion() : 0;
    }

    @Override
    public boolean supportsRedo() {
        return table != null;
    }

    @Override
    public void redo(Object obj, int metaVersion) {
        table.redo((Row) obj, metaVersion);
    }
}
