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
package org.lealone.client.session;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.DelegatedSession;
import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.storage.replication.ReplicationSession;

class AutoReconnectSession extends DelegatedSession {

    private ConnectionInfo ci;
    private String newTargetNodes;

    public AutoReconnectSession(ConnectionInfo ci) {
        if (!ci.isRemote()) {
            throw DbException.throwInternalError();
        }
        this.ci = ci;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        super.setAutoCommit(autoCommit);

        if (newTargetNodes != null) {
            reconnect();
        }
    }

    @Override
    public Session connect(boolean allowRedirect) {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            connectAsync(allowRedirect, ar -> {
                latch.countDown();
            });
            latch.await();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot connect to " + ci.getServers(), e);
        }
        return this;
    }

    public Session connect2(boolean allowRedirect) {
        InetSocketAddress inetSocketAddress = null;
        ClientSession clientSession = null;
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
        Random random = new Random(System.currentTimeMillis());
        try {
            for (int i = 0, len = servers.length; i < len; i++) {
                int randomIndex = random.nextInt(len);
                String s = servers[randomIndex];
                try {
                    clientSession = new ClientSession(ci, s, this);
                    inetSocketAddress = clientSession.open().getInetSocketAddress();
                    session = clientSession;
                    break;
                } catch (Exception e) {
                    if (i == len - 1) {
                        throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + s);
                    }
                    int index = 0;
                    String[] newServers = new String[len - 1];
                    for (int j = 0; j < len; j++) {
                        if (j != randomIndex)
                            newServers[index++] = servers[j];
                    }
                    servers = newServers;
                    len--;
                    i = -1;
                }
            }
        } catch (DbException e) {
            throw e;
        }

        if (allowRedirect) {
            if (getRunMode() == RunMode.REPLICATION) {
                ConnectionInfo ci = this.ci;
                servers = StringUtils.arraySplit(getTargetNodes(), ',', true);
                int size = servers.length;
                ClientSession[] sessions = new ClientSession[size];
                for (int i = 0; i < size; i++) {
                    // 如果首次连接的节点就是复制节点之一，则复用它
                    if (isValid()) {
                        NetNode node = NetNode.createTCP(servers[i]);
                        if (node.getInetSocketAddress().equals(inetSocketAddress)) {
                            sessions[i] = clientSession;
                            continue;
                        }
                    }
                    ci = this.ci.copy(servers[i]);
                    sessions[i] = new ClientSession(ci, servers[i], this);
                    sessions[i].open();
                }
                ReplicationSession rs = new ReplicationSession(sessions);
                rs.setAutoCommit(this.isAutoCommit());
                session = rs;
                return this;
            }
            if (isInvalid()) {
                switch (getRunMode()) {
                case CLIENT_SERVER:
                case SHARDING: {
                    this.ci = this.ci.copy(getTargetNodes());
                    // 关闭当前session,因为连到的节点不是所要的,这里可能会关闭vertx,
                    // 所以要放在构造下一个ClientSession前调用
                    this.close();
                    return connect(false);
                }
                default:
                    throw DbException.throwInternalError();
                }
            }
        }
        return this;
    }

    @Override
    public void connectAsync(boolean allowRedirect, AsyncHandler<AsyncResult<Session>> asyncHandler) {
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
        Random random = new Random(System.currentTimeMillis());
        connectAsync(servers, allowRedirect, random, asyncHandler);
    }

    private void connectAsync(String[] servers, boolean allowRedirect, Random random,
            AsyncHandler<AsyncResult<Session>> asyncHandler) {
        int randomIndex = random.nextInt(servers.length);
        String server = servers[randomIndex];
        ClientSession clientSession = new ClientSession(ci, server, this);
        clientSession.openAsync(ar -> {
            if (ar.isSucceeded()) {
                session = ar.getResult();
                InetSocketAddress inetSocketAddress = clientSession.getInetSocketAddress();
                if (allowRedirect) {
                    if (getRunMode() == RunMode.REPLICATION) {
                        ConnectionInfo ci = this.ci;
                        String[] replicationServers = StringUtils.arraySplit(getTargetNodes(), ',', true);
                        int size = replicationServers.length;

                        AtomicInteger count = new AtomicInteger();
                        CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
                        AsyncHandler<AsyncResult<Session>> replicationAsyncHandler = rar -> {
                            if (rar.isSucceeded()) {
                                sessions.add(rar.getResult());
                                if (count.incrementAndGet() == size) {
                                    ReplicationSession rs = new ReplicationSession(sessions.toArray(new Session[0]));
                                    rs.setAutoCommit(this.isAutoCommit());
                                    session = rs;
                                    asyncHandler.handle(new AsyncResult<>(session));
                                }
                            }
                        };
                        for (int i = 0; i < size; i++) {
                            // 如果首次连接的节点就是复制节点之一，则复用它
                            if (isValid()) {
                                NetNode node = NetNode.createTCP(replicationServers[i]);
                                if (node.getInetSocketAddress().equals(inetSocketAddress)) {
                                    sessions.add(clientSession);
                                    continue;
                                }
                            }
                            ci = this.ci.copy(replicationServers[i]);
                            ClientSession s = new ClientSession(ci, replicationServers[i], this);
                            s.openAsync(replicationAsyncHandler);
                        }
                        return;
                    }
                    if (isInvalid()) {
                        switch (getRunMode()) {
                        case CLIENT_SERVER:
                        case SHARDING: {
                            this.ci = this.ci.copy(getTargetNodes());
                            // 关闭当前session,因为连到的节点不是所要的,这里可能会关闭vertx,
                            // 所以要放在构造下一个ClientSession前调用
                            this.close();
                            connectAsync(false, asyncHandler);
                        }
                        default:
                            throw DbException.throwInternalError();
                        }
                    } else {
                        asyncHandler.handle(new AsyncResult<>(session));
                    }
                }
            } else {
                if (servers.length == 1) {
                    Throwable e = ar.getCause();
                    throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + server);
                }
                int i = 0;
                int len = servers.length;
                String[] newServers = new String[len - 1];
                for (int j = 0; j < len; j++) {
                    if (j != randomIndex)
                        newServers[i++] = servers[j];
                }
                connectAsync(servers, allowRedirect, random, asyncHandler);
            }
        });
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        this.newTargetNodes = newTargetNodes;
        if (session.isAutoCommit()) {
            reconnect();
        }
    }

    private void reconnect() {
        Session oldSession = this.session;
        this.ci = this.ci.copy(newTargetNodes);
        ConcurrentUtils.submitTask("Reconnect", () -> {
            connect();
            oldSession.close();
            newTargetNodes = null;
        });
    }

    @Override
    public void reconnectIfNeeded() {
        if (newTargetNodes != null) {
            reconnect();
        }
    }
}
