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
package org.lealone.transaction.amte;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public abstract class TransactionalValue {

    public final Object value;

    public TransactionalValue(Object value) {
        this.value = value;
    }

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public long getTid() {
        return 0;
    }

    public int getLogId() {
        return 0;
    }

    public boolean isLocked(int[] columnIndexes) {
        return false;
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

    public TransactionalValue getCommitted() {
        return this;
    }

    public TransactionalValue commit(long tid) {
        return this;
    }

    public boolean isCommitted() {
        return true;
    }

    public void write(DataBuffer buff, StorageDataType valueType) {
        writeMeta(buff);
        writeValue(buff, valueType);
    }

    public abstract void writeMeta(DataBuffer buff);

    public void writeValue(DataBuffer buff, StorageDataType valueType) {
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType,
            int columnCount) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = valueType.readMeta(buff, columnCount);
            return createCommitted(value);
        } else {
            return Uncommitted.readMeta(tid, valueType, buff, oldValueType, columnCount);
        }
    }

    public static Object readValue(ByteBuffer buff, StorageDataType valueType) {
        Object value = null;
        if (buff.get() == 1) {
            value = valueType.read(buff);
        }
        return value;
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = readValue(buff, valueType);
            return createCommitted(value);
        } else {
            return Uncommitted.read(tid, valueType, buff, oldValueType);
        }
    }

    public static TransactionalValue create(AMTransaction transaction, Object value, TransactionalValue oldValue,
            StorageDataType oldValueType) {
        return new Uncommitted(transaction, value, oldValue, oldValueType, null);
    }

    public static TransactionalValue create(AMTransaction transaction, Object value, TransactionalValue oldValue,
            StorageDataType oldValueType, int[] columnIndexes) {
        return new Uncommitted(transaction, value, oldValue, oldValueType, columnIndexes);
    }

    public static TransactionalValue createCommitted(Object value) {
        return new Committed(value);
    }

    static class Committed extends TransactionalValue {
        Committed(Object value) {
            super(value);
        }

        @Override
        public void writeMeta(DataBuffer buff) {
            buff.putVarLong(0);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Committed[ ");
            buff.append(value).append(" ]");
            return buff.toString();
        }
    }

    static class Uncommitted extends TransactionalValue {

        private final long tid;
        private final int logId;
        private TransactionalValue oldValue;
        private final StorageDataType oldValueType;
        private final String hostAndPort;
        // 每次修改记录的事务名要全局唯一，
        // 比如用节点的IP拼接一个本地递增的计数器组成字符串就足够了
        private final String globalReplicationName;
        private long version; // 每次更新时自动加1
        private boolean replicated;
        private boolean rowLock;
        private BitSet lockedColumns;
        private int[] columnIndexes;

        Uncommitted(AMTransaction transaction, Object value, TransactionalValue oldValue,
                StorageDataType oldValueType, int[] columnIndexes) {
            super(value);
            // 避免同一个事务对同一行不断更新导致过长的oldValue链，只取最早的oldValue即可
            if (oldValue != null) {
                if (oldValue.getTid() == transaction.transactionId && (oldValue instanceof Uncommitted)) {
                    oldValue = ((Uncommitted) oldValue).oldValue;
                } else {
                    // oldValue = oldValue.getCommitted();
                }
            }
            this.tid = transaction.transactionId;
            this.logId = transaction.logId;
            this.oldValue = oldValue;
            this.oldValueType = oldValueType;
            this.hostAndPort = NetEndpoint.getLocalTcpHostAndPort();
            this.globalReplicationName = transaction.globalTransactionName;
            this.columnIndexes = columnIndexes;

            if (columnIndexes == null || columnIndexes.length == 0) {
                rowLock = true;
            } else {
                int columnCount = oldValueType.getColumnCount();
                if (columnIndexes.length < (columnCount / 2)) {
                    rowLock = false;
                    lockedColumns = new BitSet(columnCount);
                    for (int i : columnIndexes) {
                        lockedColumns.set(i);
                    }
                } else {
                    rowLock = true;
                }
            }
        }

        Uncommitted(long tid, Object value, int logId, TransactionalValue oldValue, StorageDataType oldValueType,
                String hostAndPort, String globalTransactionName, long version) {
            super(value);
            this.tid = tid;
            this.logId = logId;
            this.oldValue = oldValue;
            this.oldValueType = oldValueType;
            this.hostAndPort = hostAndPort;
            this.globalReplicationName = globalTransactionName;
            this.version = version;
        }

        public Uncommitted copy() {
            Uncommitted u = new Uncommitted(tid, value, logId, oldValue, oldValueType, hostAndPort,
                    globalReplicationName, version);
            u.replicated = replicated;
            u.rowLock = rowLock;
            u.lockedColumns = lockedColumns;
            u.columnIndexes = columnIndexes;
            return u;
        }

        @Override
        public long getTid() {
            return tid;
        }

        @Override
        public boolean isLocked(int[] columnIndexes) {
            if (rowLock)
                return true;
            for (int i : columnIndexes) {
                if (lockedColumns.get(i))
                    return true;
            }
            // 检查多个未提交事务
            if (oldValue != null && !oldValue.isCommitted()) {
                if (oldValue.isLocked(columnIndexes)) {
                    return true;
                }
            }
            return false;
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
        public TransactionalValue getCommitted() {
            if (oldValue != null) {
                return oldValue.getCommitted();
            }
            return oldValue;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public TransactionalValue commit(long tid) {
            int[] commitColumnIndexes = null;
            Object commitValue = null;
            LinkedList<Uncommitted> uncommittedList = new LinkedList<>();
            if (tid != this.tid) {
                uncommittedList.add(this);
            } else {
                commitColumnIndexes = columnIndexes;
                commitValue = value;
            }
            TransactionalValue oldValue = this.oldValue;
            while (oldValue != null) {
                if (oldValue instanceof Uncommitted) {
                    Uncommitted u = (Uncommitted) oldValue;
                    oldValue = u.oldValue;
                    if (tid != u.tid) {
                        // 去掉当前正在提交的事务对应的条目
                        uncommittedList.add(u);
                    } else {
                        commitColumnIndexes = u.columnIndexes;
                        commitValue = u.value;
                    }
                } else {
                    oldValue = null;
                }
            }

            Uncommitted ret = null;
            Uncommitted uncommitted = null;
            for (Uncommitted u : uncommittedList) {
                u = u.copy(); // 避免多线程执行时修改原来的链接结构
                if (uncommitted == null) {
                    uncommitted = u;
                    ret = u;
                } else {
                    uncommitted.oldValue = u;
                    uncommitted = u;
                }
                if (u.value != null)
                    u.oldValueType.setColumns(u.value, commitValue, commitColumnIndexes);
            }

            if (ret == null)
                return createCommitted(value);
            else
                return ret;
        }

        private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType) {
            return read(tid, valueType, buff, oldValueType, false, 0);
        }

        private static Uncommitted readMeta(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType, int columnCount) {
            return read(tid, valueType, buff, oldValueType, true, columnCount);
        }

        private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType, boolean meta, int columnCount) {
            int logId = DataUtils.readVarInt(buff);
            boolean rowLock = buff.get() == 0;
            BitSet lockedColumns = null;
            if (!rowLock) {
                int len = DataUtils.readVarInt(buff);
                byte[] bytes = new byte[len];
                for (int i = 0; i < len; i++) {
                    bytes[i] = buff.get();
                }
                lockedColumns = BitSet.valueOf(bytes);
            }
            TransactionalValue oldValue = null;
            if (buff.get() == 1) {
                oldValue = (TransactionalValue) oldValueType.read(buff);
            }
            String hostAndPort = ValueString.type.read(buff);
            String globalReplicationName = ValueString.type.read(buff);
            long version = DataUtils.readVarLong(buff);
            Object value;
            if (meta)
                value = valueType.readMeta(buff, columnCount);
            else
                value = readValue(buff, valueType);
            Uncommitted uncommitted = new Uncommitted(tid, value, logId, oldValue, oldValueType, hostAndPort,
                    globalReplicationName, version);
            uncommitted.rowLock = rowLock;
            uncommitted.lockedColumns = lockedColumns;
            return uncommitted;
        }

        @Override
        public void writeMeta(DataBuffer buff) {
            buff.putVarLong(tid);
            buff.putVarInt(logId);
            if (rowLock) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                byte[] bytes = lockedColumns.toByteArray();
                int len = bytes.length;
                buff.putVarInt(len);
                for (int i = 0; i < len; i++) {
                    buff.put(bytes[i]);
                }
            }
            if (oldValue == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                oldValueType.write(buff, oldValue);
            }
            ValueString.type.write(buff, hostAndPort);
            ValueString.type.write(buff, globalReplicationName);
            buff.putVarLong(version);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Uncommitted[ ");
            buff.append("tid = ").append(tid);
            buff.append(", logId = ").append(logId);
            // buff.append(", version = ").append(version);
            // buff.append(", globalReplicationName = ").append(globalReplicationName);
            buff.append(", value = ").append(value).append(" ]");
            return buff.toString();
        }
    }
}
