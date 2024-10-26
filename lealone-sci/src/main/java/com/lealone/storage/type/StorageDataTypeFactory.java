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

    public static StorageDataType getLongType() {
        return getType(Value.LONG);
    }

    public static StorageDataType getType(int typeId) {
        final ValueDataTypeBase type = ObjectDataType.newType(typeId);
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
            public void write(DataBuffer buff, Object obj) {
                type.write(buff, obj);
            }

            @Override
            public void writeValue(DataBuffer buff, Value v) {
                type.writeValue(buff, v);
            }

            @Override
            public Value readValue(ByteBuffer buff, int tag) {
                return type.readValue(buff, tag);
            }
        };
    }
}
