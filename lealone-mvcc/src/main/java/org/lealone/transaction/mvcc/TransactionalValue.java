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

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class TransactionalValue {

    public final long tid; // 如果是0代表事务已经提交
    public final Object value;

    public TransactionalValue(long tid, Object value) {
        this.tid = tid;
        this.value = value;
    }

    public int getLogId() {
        return 0;
    }

    public String getHostAndPort() {
        return null;
    }

    public String getGlobalReplicationName() {
        return null;
    }

    public boolean isReplicated() {
        return false;
    }

    public void setReplicated(boolean replicated) {
    }

    public void incrementVersion() {
    }

    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        return this;
    }

    public void write(DataBuffer buff, StorageDataType valueType) {
        buff.putVarLong(tid);
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType) {
        long tid = DataUtils.readVarLong(buff);
        Object value = null;
        if (buff.get() == 1) {
            value = valueType.read(buff);
        }
        if (tid == 0) {
            return createCommitted(value);
        } else {
            return NotCommitted.read(tid, value, buff, oldValueType);
        }
    }

    public static TransactionalValue create(MVCCTransaction transaction, Object value, TransactionalValue oldValue,
            StorageDataType oldValueType) {
        return new NotCommitted(transaction, value, oldValue, oldValueType);
    }

    public static TransactionalValue createCommitted(Object value) {
        return new Committed(value);
    }

    static class Committed extends TransactionalValue {
        Committed(Object value) {
            super(0, value);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Committed[ value = ");
            buff.append(value).append(" ]");
            return buff.toString();
        }
    }

    static class NotCommitted extends TransactionalValue {

        // 每次修改记录的事务名要全局唯一，
        // 比如用节点的IP拼接一个本地递增的计数器组成字符串就足够了
        private final int logId;
        private final TransactionalValue oldValue;
        private final StorageDataType oldValueType;
        private final String hostAndPort;
        private final String globalReplicationName;
        private long version; // 每次更新时自动加1
        private boolean replicated;

        NotCommitted(MVCCTransaction transaction, Object value, TransactionalValue oldValue,
                StorageDataType oldValueType) {
            super(transaction.transactionId, value);
            this.logId = transaction.logId;
            this.oldValue = oldValue;
            this.oldValueType = oldValueType;
            this.hostAndPort = NetEndpoint.getLocalTcpHostAndPort();
            this.globalReplicationName = transaction.globalTransactionName;
        }

        NotCommitted(long tid, Object value, int logId, TransactionalValue oldValue, StorageDataType oldValueType,
                String hostAndPort, String globalTransactionName, long version) {
            super(tid, value);
            this.logId = logId;
            this.oldValue = oldValue;
            this.oldValueType = oldValueType;
            this.hostAndPort = hostAndPort;
            this.globalReplicationName = globalTransactionName;
            this.version = version;
        }

        @Override
        public int getLogId() {
            return logId;
        }

        @Override
        public String getHostAndPort() {
            return hostAndPort;
        }

        @Override
        public String getGlobalReplicationName() {
            return globalReplicationName;
        }

        @Override
        public boolean isReplicated() {
            return replicated;
        }

        @Override
        public void setReplicated(boolean replicated) {
            this.replicated = replicated;
        }

        @Override
        public void incrementVersion() {
            version++;
        }

        @Override
        public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
            if (oldValue == null) { // insert
                map.remove(key);
            } else {
                map.put(key, oldValue); // update或delete
            }
            if (oldValue != null) {
                return oldValue.undo(map, key);
            }
            return oldValue;
        }

        @Override
        public void write(DataBuffer buff, StorageDataType valueType) {
            super.write(buff, valueType);
            buff.putVarInt(logId);
            if (oldValue == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                oldValueType.write(buff, oldValue);
            }
            if (hostAndPort == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                ValueString.type.write(buff, hostAndPort);
            }
            if (globalReplicationName == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                buff.putVarLong(version);
                ValueString.type.write(buff, globalReplicationName);
            }
        }

        private static NotCommitted read(long tid, Object value, ByteBuffer buff, StorageDataType oldValueType) {
            int logId = DataUtils.readVarInt(buff);
            TransactionalValue oldValue = null;
            if (buff.get() == 1) {
                oldValue = (TransactionalValue) oldValueType.read(buff);
            }
            String hostAndPort = null;
            if (buff.get() == 1) {
                hostAndPort = ValueString.type.read(buff);
            }
            String globalReplicationName = null;
            long version = 0;
            if (buff.get() == 1) {
                version = DataUtils.readVarLong(buff);
                globalReplicationName = ValueString.type.read(buff);
            }
            return new NotCommitted(tid, value, logId, oldValue, oldValueType, hostAndPort, globalReplicationName,
                    version);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("NotCommitted[ ");
            buff.append("tid = ").append(tid);
            buff.append(", logId = ").append(logId);
            buff.append(", version = ").append(version);
            buff.append(", globalReplicationName = ").append(globalReplicationName);
            buff.append(", value = ").append(value).append(" ]");
            return buff.toString();
        }
    }
}
