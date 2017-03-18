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
package org.lealone.replication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.trace.Trace;
import org.lealone.db.Command;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.Session;
import org.lealone.db.SessionBase;
import org.lealone.sql.ParsedStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.StorageCommand;
import org.lealone.transaction.Transaction;

public class ReplicationSession extends SessionBase {
    private final Session[] sessions;

    private final String[] servers;
    private final String serversStr;

    final int n; // 复制集群节点总个数
    final int w; // 写成功的最少节点个数
    final int r; // 读成功的最少节点个数

    private final String hostName;
    private final AtomicInteger counter = new AtomicInteger(1);

    int maxRries = 5;
    long rpcTimeoutMillis = 2000L;

    public ReplicationSession(Session[] sessions) {
        this(sessions, null);
    }

    public ReplicationSession(Session[] sessions, List<String> initReplicationEndpoints) {
        this.sessions = sessions;

        String replicationEndpoints = null;
        if (initReplicationEndpoints != null) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0, size = initReplicationEndpoints.size(); i < size; i++) {
                if (i > 0)
                    buff.append('&');
                buff.append(initReplicationEndpoints.get(i));
            }
            replicationEndpoints = buff.toString();
        }

        n = sessions.length;
        w = r = n / 2 + 1;
        servers = new String[n];
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0)
                buff.append(',');
            servers[i] = sessions[i].getConnectionInfo().getServers();
            buff.append(servers[i]);
        }

        serversStr = buff.toString();

        try {
            String hostName = InetAddress.getLocalHost().getHostAddress();
            if (replicationEndpoints != null) {
                hostName = replicationEndpoints + "@" + hostName;
            }
            this.hostName = hostName;
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxRries(int maxRries) {
        this.maxRries = maxRries;
    }

    public void setRpcTimeout(long rpcTimeoutMillis) {
        this.rpcTimeoutMillis = rpcTimeoutMillis;
    }

    String createReplicationName() {
        StringBuilder n = new StringBuilder(hostName);
        n.append("_").append(System.nanoTime() / 1000).append("_").append(counter.getAndIncrement());
        n.append(',').append(serversStr);
        String replicationName = n.toString();
        for (Session s : sessions) {
            s.setReplicationName(replicationName);
        }
        return replicationName;
    }

    @Override
    public ParsedStatement parseStatement(String sql) {
        return sessions[0].parseStatement(sql);
    }

    @Override
    public Command createCommand(String sql, int fetchSize) {
        Command[] commands = new Command[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].createCommand(sql, fetchSize);
        return new ReplicationCommand(this, commands);
    }

    @Override
    public Command prepareCommand(String sql, int fetchSize) {
        Command[] commands = new Command[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].prepareCommand(sql, fetchSize);
        return new ReplicationCommand(this, commands);
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
    public PreparedStatement prepareStatement(String sql, int fetchSize) {
        return sessions[0].prepareStatement(sql, fetchSize);
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
    public boolean isAutoCommit() {
        return sessions[0].isAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        for (int i = 0; i < n; i++)
            sessions[i].setAutoCommit(autoCommit);
    }

    @Override
    public Trace getTrace() {
        return sessions[0].getTrace();
    }

    @Override
    public DataHandler getDataHandler() {
        return sessions[0].getDataHandler();
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
    public boolean isClosed() {
        return sessions[0].isClosed();
    }

    @Override
    public int getModificationId() {
        return sessions[0].getModificationId();
    }

    @Override
    public Transaction getTransaction() {
        return sessions[0].getTransaction();
    }

    @Override
    public void setTransaction(Transaction transaction) {
        for (int i = 0; i < n; i++)
            sessions[i].setTransaction(transaction);
    }

    @Override
    public void rollback() {
        for (int i = 0; i < n; i++)
            sessions[i].rollback();
    }

    @Override
    public void setRoot(boolean isRoot) {
        for (int i = 0; i < n; i++)
            sessions[i].setRoot(isRoot);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return sessions[0].validateTransaction(localTransactionName);
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        for (int i = 0; i < n; i++)
            sessions[i].commit(allLocalTransactionNames);
    }

    @Override
    public Session connectEmbeddedOrServer() {
        return sessions[0].connectEmbeddedOrServer();
    }

    @Override
    public Session connectEmbeddedOrServer(boolean first) {
        return sessions[0].connectEmbeddedOrServer(first);
    }

    @Override
    public String getURL() {
        return sessions[0].getURL();
    }

    @Override
    public String getReplicationName() {
        return sessions[0].getReplicationName();
    }

    @Override
    public void setReplicationName(String globalTransactionName) {
        sessions[0].setReplicationName(globalTransactionName);
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return sessions[0].getConnectionInfo();
    }

    @Override
    public StorageCommand createStorageCommand() {
        StorageCommand[] commands = new StorageCommand[n];
        for (int i = 0; i < n; i++)
            commands[i] = sessions[i].createStorageCommand();
        return new ReplicationCommand(this, commands);
    }
}
