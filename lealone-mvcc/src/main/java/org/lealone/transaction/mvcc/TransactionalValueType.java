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
package org.lealone.transaction.mvcc;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.storage.type.StorageDataType;

public class TransactionalValueType implements StorageDataType {

    public final StorageDataType valueType;

    public TransactionalValueType(StorageDataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        // tid最大8字节
        return 8 + valueType.getMemory(v.value);
        // TODO 由于BufferedMap的合并与复制逻辑的验证是并行的，
        // 可能导致split时三个复制节点中某些相同的TransactionalValue有些globalReplicationName为null，有些不为null
        // 这样就会得到不同的内存大小，从而使得splitKey不同
        // return valueType.getMemory(v.value) + 12
        // + (v.globalReplicationName == null ? 1 : 9 + ValueString.type.getMemory(v.globalReplicationName));
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        TransactionalValue a = (TransactionalValue) aObj;
        TransactionalValue b = (TransactionalValue) bObj;
        long comp = a.tid - b.tid;
        if (comp == 0) {
            comp = a.getLogId() - b.getLogId();
            if (comp == 0)
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
        valueType.writeMeta(buff, v.value);
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        return TransactionalValue.readMeta(buff, valueType, this, columnCount);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        valueType.writeColumn(buff, v.value, columnIndex);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        valueType.readColumn(buff, v.value, columnIndex);
    }

    @Override
    public void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        valueType.setColumns(oldObj, newObj, columnIndexes);
    }

    @Override
    public int getColumnCount() {
        return valueType.getColumnCount();
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        TransactionalValue v = (TransactionalValue) obj;
        return valueType.getMemory(v.value, columnIndex);
    }
}
