/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.type;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDataTypeBase;

public class StorageDataTypeFactory {

    private StorageDataTypeFactory() {
    }

    public static StorageDataType getIntType() {
        return getType(Value.INT);
    }

    public static StorageDataType getLongType() {
        return getType(Value.LONG);
    }

    public static StorageDataType getFloatType() {
        return getType(Value.FLOAT);
    }

    public static StorageDataType getDoubleType() {
        return getType(Value.DOUBLE);
    }

    public static StorageDataType getStringType() {
        return getType(Value.STRING);
    }

    public static StorageDataType getObjectType() {
        return getStorageDataType(new ObjectDataType());
    }

    public static StorageDataType getType(int typeId) {
        ValueDataTypeBase type = ObjectDataType.newType(typeId);
        return getStorageDataType(type);
    }

    private static StorageDataType getStorageDataType(final ValueDataTypeBase type) {

        return new StorageDataTypeBase() {

            @Override
            public int getType() {
                return type.getType();
            }

            @Override
            public int compare(Object aObj, Object bObj) {
                return type.compare(aObj, bObj);
            }

            @Override
            public int getMemory(Object obj) {
                return type.getMemory(obj);
            }

            @Override
            public void write(DataBuffer buff, Object obj, int formatVersion) {
                type.write(buff, obj, formatVersion);
            }

            @Override
            public void writeValue(DataBuffer buff, Value v) {
                type.writeValue(buff, v);
            }

            @Override
            public Object read(ByteBuffer buff, int formatVersion) {
                return type.read(buff, formatVersion);
            }

            @Override
            public Object read(ByteBuffer buff, int tag, int formatVersion) {
                return type.read(buff, tag, formatVersion);
            }

            @Override
            public Value readValue(ByteBuffer buff, int tag) {
                return type.readValue(buff, tag);
            }
        };
    }
}
