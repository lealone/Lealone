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
package org.lealone.storage.replication;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.session.DelegatedSession;
import org.lealone.db.session.Session;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.StorageCommand;
import org.lealone.transaction.Transaction;

public class ReplicationSession extends DelegatedSession {

    private final Session[] sessions;
    private final String[] servers;
    private final String serversStr;

    private final String replicationNamePrefix;
    private final AtomicInteger counter = new AtomicInteger(1);

    private ConsistencyLevel consistencyLevel;

    final int n; // 复制集群节点总个数

    // 当改变ConsistencyLevel时这两个字段也会随之改动
    int r; // 读成功的最少节点个数
    int w; // 写成功的最少节点个数

    int maxTries = 5;

    public ReplicationSession(Session[] sessions) {
        this(sessions, null);
    }

    public ReplicationSession(Session[] sessions, List<String> initReplicationNodes) {
        super(sessions[0]);
        this.sessions = sessions;

        String replicationNodes = null;
        if (initReplicationNodes != null) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0, size = initReplicationNodes.size(); i < size; i++) {
                if (i > 0)
                    buff.append('&');
                buff.append(initReplicationNodes.get(i));
            }
            replicationNodes = buff.toString();
        }

        n = sessions.length;
        servers = new String[n];
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0)
                buff.append(',');
            servers[i] = sessions[i].getConnectionInfo().getServers();
            buff.append(servers[i]);
        }
        serversStr = buff.toString();

        String replicationNamePrefix = getLocalHostAndPort() + "_" + getId() + "_";
        if (replicationNodes != null) {
            replicationNamePrefix = replicationNodes + "@" + replicationNamePrefix;
        }
        this.replicationNamePrefix = replicationNamePrefix;

        // 设置默认级别
        setConsistencyLevel(ConsistencyLevel.ALL);
    }

    public void setMaxTries(int maxTries) {
        this.maxTries = maxTries;
    }

    public void setRpcTimeout(int rpcTimeoutMillis) {
        for (Session s : sessions) {
            s.setNetworkTimeout(rpcTimeoutMillis);
        }
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        // 使用Write all read one模式
        if (consistencyLevel == ConsistencyLevel.ALL) {
            w = n;
            r = 1;
        } else {
            w = r = n / 2 + 1;
        }
    }

    // 复制名的格式: hostName:port_sessionId_time_counter,consistencyLevel,serversStr
    String createReplicationName() {
        StringBuilder n = new StringBuilder(replicationNamePrefix);
        n.append(System.nanoTime() / 1000).append("_").append(counter.getAndIncrement());
        n.append(',').append(consistencyLevel.code).append(',').append(serversStr);
        String replicationName = n.toString();
        for (Session s : sessions) {
            s.setReplicationName(replicationName);
        }
        return replicationName;
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize) {
        ReplicaSQLCommand[] commands = new ReplicaSQLCommand[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].createReplicaSQLCommand(sql, fetchSize);
        return new ReplicationSQLCommand(this, commands);
    }

    @Override
    public SQLCommand prepareSQLCommand(String sql, int fetchSize) {
        ReplicaSQLCommand[] commands = new ReplicaSQLCommand[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].prepareReplicaSQLCommand(sql, fetchSize);
        return new ReplicationSQLCommand(this, commands);
    }

    @Override
    public StorageCommand createStorageCommand() {
        ReplicaStorageCommand[] commands = new ReplicaStorageCommand[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].createReplicaStorageCommand();
        return new ReplicationStorageCommand(this, commands);
    }

    @Override
    public void addSavepoint(String name) {
        for (int i = 0; i < n; i++)
            sessions[i].addSavepoint(name);
    }

    @Override
    public void rollbackToSavepoint(String name) {
        for (int i = 0; i < n; i++)
            sessions[i].rollbackToSavepoint(name);
    }

    @Override
    public void commitTransaction(String localTransactionName) {
        for (int i = 0; i < n; i++)
            sessions[i].commitTransaction(localTransactionName);
    }

    @Override
    public void rollbackTransaction() {
        for (int i = 0; i < n; i++)
            sessions[i].rollbackTransaction();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        for (int i = 0; i < n; i++)
            sessions[i].setAutoCommit(autoCommit);
        super.setAutoCommit(autoCommit);
    }

    @Override
    public void cancel() {
        for (int i = 0; i < n; i++)
            sessions[i].cancel();
    }

    @Override
    public void close() {
        for (int i = 0; i < n; i++)
            sessions[i].close();
    }

    @Override
    public void setParentTransaction(Transaction transaction) {
        for (int i = 0; i < n; i++)
            sessions[i].setParentTransaction(transaction);
    }
}
