/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.session;

import java.util.Random;

import com.lealone.client.ClientScheduler;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.StringUtils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionFactoryBase;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetClient;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetFactory;
import com.lealone.net.NetNode;
import com.lealone.net.TcpClientConnection;
import com.lealone.server.protocol.AckPacketHandler;
import com.lealone.server.protocol.session.SessionInit;
import com.lealone.server.protocol.session.SessionInitAck;

public class ClientSessionFactory extends SessionFactoryBase {

    private static final ClientSessionFactory instance = new ClientSessionFactory();

    public static ClientSessionFactory getInstance() {
        return instance;
    }

    @Override
    public Future<Session> createSession(ConnectionInfo ci, boolean allowRedirect) {
        if (!ci.isRemote()) {
            throw DbException.getInternalError();
        }
        AsyncCallback<Session> ac;
        NetClient netClient;
        CaseInsensitiveMap<String> config = ci.getConfig();
        NetFactory netFactory = NetFactory.getFactory(config);
        if (netFactory.isBio()) {
            ac = AsyncCallback.create(true);
            netClient = netFactory.createNetClient();
            ci.setSingleThreadCallback(true);
            createSession(ci, allowRedirect, ac, config, netClient);
        } else {
            Scheduler scheduler = ci.getScheduler();
            if (scheduler == null)
                scheduler = ClientScheduler.getScheduler(ci, config);
            NetEventLoop eventLoop = (NetEventLoop) scheduler.getNetEventLoop();
            netClient = eventLoop.getNetClient();
            ac = AsyncCallback.create(ci.isSingleThreadCallback());
            scheduler.handle(() -> {
                createSession(ci, allowRedirect, ac, config, netClient);
            });
        }
        return ac;
    }

    private static void createSession(ConnectionInfo ci, boolean allowRedirect,
            AsyncCallback<Session> ac, CaseInsensitiveMap<String> config, NetClient netClient) {
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',');
        Random random = new Random(System.currentTimeMillis());
        createSession(ci, servers, allowRedirect, random, ac, config, netClient);
    }

    // servers是接入节点，可以有多个，会随机选择一个进行连接，这个被选中的接入节点可能不是所要连接的数居库所在的节点，
    // 这时接入节点会返回数据库的真实所在节点，最后再根据数据库的运行模式打开合适的连接即可，
    // 复制模式需要打开所有节点，其他运行模式只需要打开一个。
    // 如果第一次从servers中随机选择的一个连接失败了，会尝试其他的，当所有尝试都失败了才会抛出异常。
    private static void createSession(ConnectionInfo ci, String[] servers, boolean allowRedirect,
            Random random, AsyncCallback<Session> topAc, CaseInsensitiveMap<String> config,
            NetClient netClient) {
        int randomIndex = random.nextInt(servers.length);
        String server = servers[randomIndex];
        AsyncCallback<ClientSession> ac = AsyncCallback.createSingleThreadCallback();
        ac.onComplete(ar -> {
            if (ar.isSucceeded()) {
                ClientSession clientSession = ar.getResult();
                // 看看是否需要根据运行模式从当前接入节点转到数据库所在的节点
                if (allowRedirect) {
                    redirectIfNeeded(clientSession, ci, topAc, config, netClient);
                } else {
                    sessionCreated(clientSession, ci, topAc);
                }
            } else {
                // 如果已经是最后一个了那就可以直接抛异常了，否则再选其他的
                if (servers.length == 1) {
                    Throwable e = DbException.getCause(ar.getCause());
                    e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, server);
                    topAc.setAsyncResult(e);
                } else {
                    int len = servers.length;
                    String[] newServers = new String[len - 1];
                    for (int i = 0, j = 0; j < len; j++) {
                        if (j != randomIndex)
                            newServers[i++] = servers[j];
                    }
                    createSession(ci, newServers, allowRedirect, random, topAc, config, netClient);
                }
            }
        });
        createClientSession(ci, server, ac, config, netClient);
    }

    private static void createClientSession(ConnectionInfo ci, String server,
            AsyncCallback<ClientSession> ac, CaseInsensitiveMap<String> config, NetClient netClient) {
        NetNode node = NetNode.createTCP(server);
        // 多个客户端session会共用同一条TCP连接
        netClient.createConnection(config, node, ci.getScheduler()).onComplete(ar -> {
            if (ar.isSucceeded()) {
                AsyncConnection conn = ar.getResult();
                if (!(conn instanceof TcpClientConnection)) {
                    RuntimeException e = DbException
                            .getInternalError("not tcp client connection: " + conn.getClass().getName());
                    ac.setAsyncResult(e);
                    return;
                }

                TcpClientConnection tcpConnection = (TcpClientConnection) conn;
                // 每一个通过网络传输的协议包都会带上sessionId，
                // 这样就能在同一条TCP连接中区分不同的客户端session了
                int sessionId = tcpConnection.getNextId();
                ClientSession clientSession = new ClientSession(tcpConnection, ci, server, sessionId);
                clientSession.setSingleThreadCallback(ci.isSingleThreadCallback());
                tcpConnection.addSession(sessionId, clientSession);

                SessionInit packet = new SessionInit(ci);
                AckPacketHandler<ClientSession, SessionInitAck> ackPacketHandler = ack -> {
                    clientSession.setProtocolVersion(ack.clientVersion);
                    clientSession.setAutoCommit(ack.autoCommit);
                    clientSession.setTargetNodes(ack.targetNodes);
                    clientSession.setRunMode(ack.runMode);
                    clientSession.setInvalid(ack.invalid);
                    clientSession.setConsistencyLevel(ack.consistencyLevel);
                    return clientSession;
                };
                Future<ClientSession> f = clientSession.send(packet, ackPacketHandler);
                f.onComplete(ar2 -> {
                    ac.setAsyncResult(ar2);
                });
            } else {
                ac.setAsyncResult(ar.getCause());
            }
        });
    }

    private static void redirectIfNeeded(ClientSession clientSession, ConnectionInfo ci,
            AsyncCallback<Session> topAc, CaseInsensitiveMap<String> config, NetClient netClient) {
        if (clientSession.isInvalid()) {
            switch (clientSession.getRunMode()) {
            case CLIENT_SERVER:
            case SHARDING: {
                ConnectionInfo ci2 = ci.copy(clientSession.getTargetNodes());
                // 关闭当前session,因为连到的节点不是所要的
                clientSession.close();
                createSession(ci2, false, topAc, config, netClient);
                break;
            }
            default:
                topAc.setAsyncResult(DbException.getInternalError());
            }
        } else {
            sessionCreated(clientSession, ci, topAc);
        }
    }

    private static void sessionCreated(ClientSession clientSession, ConnectionInfo ci,
            AsyncCallback<Session> topAc) {
        Session session = clientSession;
        if (ci.getProperty(ConnectionSetting.AUTO_RECONNECT, false)) {
            session = new AutoReconnectSession(ci, session);
        }
        topAc.setAsyncResult(session);
    }
}
