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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class TransactionalLogRecord {

    final String mapName;
    Object key; // 没有用final，在MVCCTransaction.replicationPrepareCommit方法那里有特殊用途
    final TransactionalValue oldValue;
    final TransactionalValue newValue;

    public TransactionalLogRecord(String mapName, Object key, TransactionalValue oldValue,
            TransactionalValue newValue) {
        this.mapName = mapName;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
    public void commit(MVCCTransactionEngine transactionEngine, long tid) {
        StorageMap<Object, TransactionalValue> map = transactionEngine.getMap(mapName);
        if (map == null) {
            // map was later removed
        } else {
            TransactionalValue value = map.get(key);
            if (value == null) {
                // nothing to do
            } else if (value.value == null) {
                // remove the value
                map.remove(key);
            } else {
                // map.put(key, TransactionalValue.createCommitted(value.value));
                // map.put(key, value.commit());
                TransactionalValue newValue = value.commit(tid);
                while (!map.replace(key, value, newValue)) {
                    value = map.get(key);
                    newValue = value.commit(tid);
                }
            }
        }
    }

    // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
    public void rollback(MVCCTransactionEngine transactionEngine) {
        StorageMap<Object, TransactionalValue> map = transactionEngine.getMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map != null) {
            if (oldValue == null) {
                // this transaction added the value
                map.remove(key);
            } else {
                // this transaction updated the value
                map.put(key, oldValue);
            }
        }
    }

    // 用于redo时，不关心oldValue
    public void writeForRedo(DataBuffer writeBuffer, MVCCTransactionEngine transactionEngine) {
        StorageMap<?, ?> map = transactionEngine.getMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map == null) {
            return;
        }
        int lastPosition = writeBuffer.position();

        ValueString.type.write(writeBuffer, mapName);
        int keyValueLengthStartPos = writeBuffer.position();
        writeBuffer.putInt(0);

        map.getKeyType().write(writeBuffer, key);
        if (newValue.value == null)
            writeBuffer.put((byte) 0);
        else {
            writeBuffer.put((byte) 1);
            ((TransactionalValueType) map.getValueType()).valueType.write(writeBuffer, newValue.value);
        }
        writeBuffer.putInt(keyValueLengthStartPos, writeBuffer.position() - keyValueLengthStartPos - 4);

        // 预估一下内存占用大小，当到达一个阈值时方便其他服务线程刷数据到硬盘
        int memory = writeBuffer.position() - lastPosition;
        transactionEngine.incrementEstimatedMemory(mapName, memory);
    }

    // 这个方法在数据库初始化读取redo日志时调用，此时还没有打开底层存储的map，所以只预先解析出mapName和keyValue字节数组
    public static void readForRedo(ByteBuffer buff, Map<String, List<ByteBuffer>> pendingRedoLog) {
        String mapName = ValueString.type.read(buff);

        List<ByteBuffer> keyValues = pendingRedoLog.get(mapName);
        if (keyValues == null) {
            keyValues = new ArrayList<>();
            pendingRedoLog.put(mapName, keyValues);
        }
        int len = buff.getInt();
        byte[] keyValue = new byte[len];
        buff.get(keyValue);
        keyValues.add(ByteBuffer.wrap(keyValue));
    }

    // 第一次打开底层存储的map时调用这个方法，重新执行一次上次已经成功并且在检查点之后的事务操作
    @SuppressWarnings("unchecked")
    public static <K> void redo(StorageMap<K, TransactionalValue> map, List<ByteBuffer> pendingKeyValues) {
        if (pendingKeyValues != null && !pendingKeyValues.isEmpty()) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = ((TransactionalValueType) map.getValueType()).valueType;
            for (ByteBuffer kv : pendingKeyValues) {
                K key = (K) kt.read(kv);
                if (kv.get() == 0)
                    map.remove(key);
                else {
                    Object value = vt.read(kv);
                    map.put(key, TransactionalValue.createCommitted(value));
                }
            }
        }
    }
}
