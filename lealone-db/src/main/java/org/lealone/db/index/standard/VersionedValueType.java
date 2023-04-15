/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.index.standard;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.storage.type.StorageDataType;

public class VersionedValueType implements StorageDataType {

    final ValueDataType valueType;
    final int columnCount;

    public VersionedValueType(ValueDataType valueType, int columnCount) {
        this.valueType = valueType;
        this.columnCount = columnCount;
    }

    @Override
    public int getMemory(Object obj) {
        VersionedValue v = (VersionedValue) obj;
        int memory = 4;
        if (v == null)
            return memory;
        Value[] columns = v.columns;
        for (int i = 0, len = columns.length; i < len; i++) {
            Value c = columns[i];
            if (c == null)
                memory += 4;
            else
                memory += valueType.getMemory(c);
        }
        return memory;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        VersionedValue a = (VersionedValue) aObj;
        VersionedValue b = (VersionedValue) bObj;
        long comp = a.version - b.version;
        if (comp == 0) {
            return valueType.compareValues(a.columns, b.columns);
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        int vertion = DataUtils.readVarInt(buff);
        ValueArray value = (ValueArray) valueType.read(buff);
        return new VersionedValue(vertion, value.getList());
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.version);
        valueType.write(buff, ValueArray.get(v.columns));
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.version);
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        int vertion = DataUtils.readVarInt(buff);
        Value[] columns = new Value[columnCount];
        return new VersionedValue(vertion, columns);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length)
            buff.writeValue(columns[columnIndex]);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length) {
            Value value = (Value) valueType.read(buff);
            columns[columnIndex] = value;
        }
    }

    @Override
    public void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        if (columnIndexes != null) {
            VersionedValue oldValue = (VersionedValue) oldObj;
            VersionedValue newValue = (VersionedValue) newObj;
            Value[] oldColumns = oldValue.columns;
            Value[] newColumns = newValue.columns;
            for (int i : columnIndexes) {
                oldColumns[i] = newColumns[i];
            }
        }
    }

    @Override
    public ValueArray getColumns(Object obj) {
        return ValueArray.get(((VersionedValue) obj).columns);
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length) {
            return valueType.getMemory(columns[columnIndex]);
        } else {
            return 0;
        }
    }
}
