/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.db.index;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.storage.type.StorageDataType;

public class VersionedValueType implements StorageDataType {

    final StorageDataType valueType;
    final int columnCount;

    public VersionedValueType(StorageDataType valueType, int columnCount) {
        this.valueType = valueType;
        this.columnCount = columnCount;
    }

    @Override
    public int getMemory(Object obj) {
        VersionedValue v = (VersionedValue) obj;
        int memory = 4;
        if (v == null)
            return memory;
        Value[] columns = v.value.getList();
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
        long comp = a.vertion - b.vertion;
        if (comp == 0) {
            return valueType.compare(a.value, b.value);
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
        return new VersionedValue(vertion, value);
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
        buff.putVarInt(v.vertion);
        valueType.write(buff, v.value);
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.vertion);
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        int vertion = DataUtils.readVarInt(buff);
        Value[] values = new Value[columnCount];
        return new VersionedValue(vertion, ValueArray.get(values));
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.value.getList();
        if (columnIndex >= 0 && columnIndex < columns.length)
            buff.writeValue(columns[columnIndex]);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.value.getList();
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
            Value[] oldColumns = oldValue.value.getList();
            Value[] newColumns = newValue.value.getList();
            for (int i : columnIndexes) {
                oldColumns[i] = newColumns[i];
            }
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.value.getList();
        if (columnIndex >= 0 && columnIndex < columns.length) {
            return valueType.getMemory(columns[columnIndex]);
        } else {
            return 0;
        }
    }
}
