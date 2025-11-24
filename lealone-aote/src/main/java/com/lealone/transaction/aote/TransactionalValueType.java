/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.value.ValueArray;
import com.lealone.storage.type.StorageDataType;

public class TransactionalValueType implements StorageDataType {

    private final StorageDataType valueType;
    private final boolean isByteStorage;

    public TransactionalValueType(StorageDataType valueType, boolean isByteStorage) {
        this.valueType = valueType;
        this.isByteStorage = isByteStorage;
    }

    @Override
    public int getMemory(Object obj) {
        Lockable lockable = (Lockable) obj;
        if (lockable instanceof TransactionalValue) {
            obj = Lock.getLockedValue(lockable);
            if (obj == null)
                return 16;
            else
                return 16 + valueType.getMemory(obj);
        } else {
            return valueType.getMemory(lockable);
        }
    }

    @Override
    public int getColumnsMemory(Object obj) {
        return valueType.getColumnsMemory(obj);
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        Lockable a = (Lockable) aObj;
        Lockable b = (Lockable) bObj;
        long comp = TransactionalValue.getTid(a) - TransactionalValue.getTid(b);
        if (comp == 0) {
            return valueType.compare(a.getValue(), b.getValue());
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, int formatVersion) {
        for (int i = 0; i < len; i++) {
            obj[i] = valueType.merge(obj[i], read(buff, formatVersion));
        }
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        return TransactionalValue.read(buff, valueType, this, formatVersion);
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len, int formatVersion) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i], formatVersion);
        }
    }

    @Override
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        Lockable lockable = (Lockable) obj;
        TransactionalValue.write(lockable, buff, valueType, isByteStorage, formatVersion);
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj, int formatVersion) {
        Lockable lockable = (Lockable) obj;
        TransactionalValue.writeMeta(lockable, buff, formatVersion);
        valueType.writeMeta(buff, lockable.getValue(), formatVersion);
    }

    @Override
    public Object readMeta(ByteBuffer buff, Object obj, int columnCount, int formatVersion) {
        return TransactionalValue.readMeta(buff, valueType, this, obj, columnCount, formatVersion);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex, int formatVersion) {
        Lockable v = (Lockable) obj;
        valueType.writeColumn(buff, v.getValue(), columnIndex, formatVersion);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex, int formatVersion) {
        Lockable v = (Lockable) obj;
        valueType.readColumn(buff, v.getValue(), columnIndex, formatVersion);
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
        Lockable v = (Lockable) obj;
        return valueType.getMemory(v.getValue(), columnIndex);
    }

    @Override
    public Object convertToIndexKey(Object key, Object value) {
        return valueType.convertToIndexKey(key, value);
    }

    @Override
    public boolean isLockable() {
        return true;
    }

    @Override
    public boolean isKeyOnly() {
        return valueType.isKeyOnly();
    }

    @Override
    public boolean isRowOnly() {
        return valueType.isRowOnly();
    }

    @Override
    public void setRowOnly(boolean rowOnly) {
        valueType.setRowOnly(rowOnly);
    }

    @Override
    public StorageDataType getRawType() {
        return valueType;
    }
}
