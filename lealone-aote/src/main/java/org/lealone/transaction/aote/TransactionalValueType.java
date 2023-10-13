/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueArray;
import org.lealone.storage.type.StorageDataType;

public class TransactionalValueType implements StorageDataType {

    public final StorageDataType valueType;

    public TransactionalValueType(StorageDataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        TransactionalValue tv = (TransactionalValue) obj;
        Object v = tv.getValue();
        if (v == null) // 如果记录已经删除，看看RowLock中是否还有
            v = tv.getOldValue();
        return 8 + valueType.getMemory(v);
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        TransactionalValue a = (TransactionalValue) aObj;
        TransactionalValue b = (TransactionalValue) bObj;
        long comp = a.getTid() - b.getTid();
        if (comp == 0) {
            return valueType.compare(a.getValue(), b.getValue());
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
        return TransactionalValue.read(buff, valueType, this);
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        v.write(buff, valueType);
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        v.writeMeta(buff);
        valueType.writeMeta(buff, v.getValue());
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        return TransactionalValue.readMeta(buff, valueType, this, columnCount);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        valueType.writeColumn(buff, v.getValue(), columnIndex);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        valueType.readColumn(buff, v.getValue(), columnIndex);
    }

    @Override
    public void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        valueType.setColumns(oldObj, newObj, columnIndexes);
    }

    @Override
    public ValueArray getColumns(Object obj) {
        return valueType.getColumns(obj);
    }

    @Override
    public int getColumnCount() {
        return valueType.getColumnCount();
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        return valueType.getMemory(v.getValue(), columnIndex);
    }
}
