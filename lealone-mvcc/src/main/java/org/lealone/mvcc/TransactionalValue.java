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
package org.lealone.mvcc;

public class TransactionalValue {

    // 每次修改记录的事务名要全局唯一，
    // 比如用节点的IP拼接一个本地递增的计数器组成字符串就足够了
    public final String globalReplicationName;
    public final long tid;
    public final int logId;
    public final Object value;

    public long version; // 每次更新时自动加1
    public boolean replicated;

    public TransactionalValue(Object value) {
        this(0, 0, value);
    }

    public TransactionalValue(long tid, int logId, Object value) {
        this.tid = tid;
        this.logId = logId;
        this.globalReplicationName = null;
        this.value = value;
    }

    public TransactionalValue(MVCCTransaction transaction, Object value) {
        this.tid = transaction.transactionId;
        this.logId = transaction.logId;
        this.globalReplicationName = transaction.globalTransactionName;
        this.value = value;
    }

    public TransactionalValue(long tid, int logId, Object value, long version, String globalTransactionName) {
        this.tid = tid;
        this.logId = logId;
        this.value = value;
        this.version = version;
        this.globalReplicationName = globalTransactionName;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("TransactionalValue[ ");
        buff.append("version = ").append(version);
        buff.append(", globalReplicationName = ").append(globalReplicationName);
        buff.append(", tid = ").append(tid);
        buff.append(", logId = ").append(logId);
        buff.append(", value = ").append(value).append(" ]");
        return buff.toString();
    }
}
