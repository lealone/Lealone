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
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.StringUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;
import org.lealone.server.protocol.session.SessionInit;
import org.lealone.server.protocol.session.SessionInitAck;
import org.lealone.storage.replication.ReplicationSession;

public class ClientSessionFactory implements SessionFactory {

    private static final ClientSessionFactory instance = new ClientSessionFactory();

    public static ClientSessionFactory getInstance() {
        return instance;
    }

    private ClientSessionFactory() {
    }

    @Override
    public Session createSession(ConnectionInfo ci) {
        return createSession(ci, true);
    }

    @Override
    public Session createSession(ConnectionInfo ci, boolean allowRedirect) {
        try {
            AtomicReference<Session> r = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            createSessionAsync(ci, allowRedirect, ar -> {
                r.set(ar.getResult());
                latch.countDown();
            });
            latch.await();
            return r.get();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot connect to " + ci.getServers(), e);
        }
    }

    @Override
    public void createSessionAsync(ConnectionInfo ci, boolean allowRedirect,
            AsyncHandler<AsyncResult<Session>> asyncHandler) {
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
        Random random = new Random(System.currentTimeMillis());
        AutoReconnectSession parent = new AutoReconnectSession(ci);
        createSessionAsync(parent, ci, servers, allowRedirect, random, asyncHandler);
    }

    // servers是接入节点，可以有多个，会随机选择一个进行连接，这个被选中的接入节点可能不是所要连接的数居库所在的节点，
    // 这时接入节点会返回数据库的真实所在节点，最后再根据数据库的运行模式打开合适的连接即可，
    // 复制模式需要打开所有节点，其他运行模式只需要打开一个。
    // 如果第一次从servers中随机选择的一个连接失败了，会尝试其他的，当所有尝试都失败了才会抛出异常。
    private void createSessionAsync(AutoReconnectSession parent, ConnectionInfo ci, String[] servers,
            boolean allowRedirect, Random random, AsyncHandler<AsyncResult<Session>> asyncHandler) {
        int randomIndex = random.nextInt(servers.length);
        String server = servers[randomIndex];
        createClientSessionAsync(parent, ci, server, ar -> {
            if (ar.isSucceeded()) {
                ClientSession clientSession = ar.getResult();
                // 看看是否需要根据运行模式从当前接入节点转到数据库所在的节点
                if (allowRedirect) {
                    redirectIfNeeded(parent, clientSession, ci, asyncHandler);
                } else {
                    parent.setSession(clientSession);
                    asyncHandler.handle(new AsyncResult<>(parent));
                }
            } else {
                // 如果已经是最后一个了那就可以直接抛异常了，否则再选其他的
                if (servers.length == 1) {
                    Throwable e = ar.getCause();
                    e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + server);
                    asyncHandler.handle(new AsyncResult<>(e));
                } else {
                    int i = 0;
                    int len = servers.length;
                    String[] newServers = new String[len - 1];
                    for (int j = 0; j < len; j++) {
                        if (j != randomIndex)
                            newServers[i++] = servers[j];
                    }
                    createSessionAsync(parent, ci, newServers, allowRedirect, random, asyncHandler);
                }
            }
        });
    }

    private void createClientSessionAsync(AutoReconnectSession parent, ConnectionInfo ci, String server,
            AsyncHandler<AsyncResult<ClientSession>> asyncHandler) {
        NetNode node = NetNode.createTCP(server);
        NetFactory factory = NetFactoryManager.getFactory(ci.getNetFactoryName());
        CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>(ci.getProperties());
        // 多个客户端session会共用同一条TCP连接
        factory.getNetClient().createConnectionAsync(config, node, ar -> {
            if (ar.isSucceeded()) {
                AsyncConnection conn = ar.getResult();
                if (!(conn instanceof TcpClientConnection)) {
                    RuntimeException e = DbException
                            .throwInternalError("not tcp client connection: " + conn.getClass().getName());
                    asyncHandler.handle(new AsyncResult<>(e));
                    return;
                }

                TcpClientConnection tcpConnection = (TcpClientConnection) conn;
                // 每一个通过网络传输的协议包都会带上sessionId，
                // 这样就能在同一条TCP连接中区分不同的客户端session了
                int sessionId = tcpConnection.getNextId();
                ClientSession clientSession = new ClientSession(ci, server, parent, tcpConnection, sessionId);

                SessionInit packet = new SessionInit(ci);
                clientSession.<SessionInitAck> sendAsync(packet, ack -> {
                    clientSession.setProtocolVersion(ack.clientVersion);
                    clientSession.setAutoCommit(ack.autoCommit);
                    clientSession.setTargetNodes(ack.targetNodes);
                    clientSession.setRunMode(ack.runMode);
                    clientSession.setInvalid(ack.invalid);
                    asyncHandler.handle(new AsyncResult<>(clientSession));
                });
            } else {
                asyncHandler.handle(new AsyncResult<>(ar.getCause()));
            }
        });
    }

    private void redirectIfNeeded(AutoReconnectSession parent, ClientSession clientSession, final ConnectionInfo ci,
            AsyncHandler<AsyncResult<Session>> asyncHandler) {
        if (clientSession.getRunMode() == RunMode.REPLICATION) {
            String[] replicationServers = StringUtils.arraySplit(clientSession.getTargetNodes(), ',', true);
            int size = replicationServers.length;
            AtomicInteger count = new AtomicInteger();
            CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
            AsyncHandler<AsyncResult<ClientSession>> replicationAsyncHandler = rar -> {
                if (rar.isSucceeded()) {
                    sessions.add(rar.getResult());
                    if (count.incrementAndGet() == size) {
                        ReplicationSession rs = new ReplicationSession(sessions.toArray(new Session[0]));
                        rs.setAutoCommit(clientSession.isAutoCommit());
                        parent.setSession(rs);
                        asyncHandler.handle(new AsyncResult<>(parent));
                    }
                } else {
                    count.incrementAndGet();
                }
            };

            InetSocketAddress inetSocketAddress = clientSession.getInetSocketAddress();
            for (int i = 0; i < size; i++) {
                // 如果首次连接的节点就是复制节点之一，则复用它
                if (clientSession.isValid()) {
                    NetNode node = NetNode.createTCP(replicationServers[i]);
                    if (node.getInetSocketAddress().equals(inetSocketAddress)) {
                        sessions.add(clientSession);
                        count.incrementAndGet();
                        continue;
                    }
                }
                ConnectionInfo ci2 = ci.copy(replicationServers[i]);
                createClientSessionAsync(parent, ci2, replicationServers[i], replicationAsyncHandler);
            }
        } else {
            if (clientSession.isInvalid()) {
                switch (clientSession.getRunMode()) {
                case CLIENT_SERVER:
                case SHARDING: {
                    ConnectionInfo ci2 = ci.copy(clientSession.getTargetNodes());
                    // 关闭当前session,因为连到的节点不是所要的
                    clientSession.close();
                    createSessionAsync(ci2, false, asyncHandler);
                }
                default:
                    asyncHandler.handle(new AsyncResult<>(DbException.throwInternalError()));
                }
            } else {
                parent.setSession(clientSession);
                asyncHandler.handle(new AsyncResult<>(parent));
            }
        }
    }
}
