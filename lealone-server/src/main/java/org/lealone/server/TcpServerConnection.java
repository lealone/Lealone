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
package org.lealone.server;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ExpiringMap;
import org.lealone.common.util.Pair;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.SysProperties;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.net.TransferConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler.SessionInfo;
import org.lealone.server.handler.LobPacketHandlers.LobCache;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.session.SessionInit;
import org.lealone.server.protocol.session.SessionInitAck;

/**
 * 这里只处理客户端通过TCP连到服务器端后的协议，可以在一个TCP连接中打开多个session
 * 
 */
// 注意: 以下代码中出现的sessionId都表示客户端session的id，
// 调用createSession创建的是服务器端的session，这个session的id有可能跟客户端session的id不一样，
// 但是可以把客户端session的id跟服务器端的session做一个影射，这样两端的session就对上了。
public class TcpServerConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpServerConnection.class);

    // 每个sessionId对应一个专有的SessionInfo，
    // 所有与这个sessionId相关的命令请求都先放到SessionInfo中的队列，
    // 然后由调度器根据优先级从多个队列中依次取出执行。
    private final ConcurrentHashMap<Integer, SessionInfo> sessions = new ConcurrentHashMap<>();
    private final TcpServer tcpServer;
    private final ExpiringMap<Integer, AutoCloseable> cache; // 缓存PreparedStatement和结果集
    private LobCache lobCache; // 大多数情况下都不使用lob，所以延迟初始化

    public TcpServerConnection(TcpServer tcpServer, WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        this.tcpServer = tcpServer;
        cache = new ExpiringMap<>(ScheduleService.getScheduler(), tcpServer.getSessionTimeout(),
                new Function<Pair<Integer, ExpiringMap.CacheableObject<AutoCloseable>>, Void>() {
                    @Override
                    public Void apply(Pair<Integer, ExpiringMap.CacheableObject<AutoCloseable>> pair) {
                        try {
                            pair.right.value.close();
                        } catch (Exception e) {
                            logger.warn(e.getMessage());
                        }
                        return null;
                    }
                });
    }

    // 这个方法是由网络事件循环线程执行的
    @Override
    protected void handleRequest(TransferInputStream in, int packetId, int packetType) throws IOException {
        // 这里的sessionId是客户端session的id，每个数据包都会带这个字段
        int sessionId = in.readInt();
        SessionInfo si = sessions.get(sessionId);
        if (si == null) {
            if (packetType == PacketType.SESSION_INIT.value) {
                // 同一个session的所有请求包(含InitPacket)都由同一个调度器负责处理
                // 新session创建成功后再回填session字段
                SessionInfo newSi = new SessionInfo(this, null, sessionId, tcpServer.getSessionTimeout());
                newSi.submitTask(() -> {
                    readInitPacket(in, packetId, sessionId, newSi);
                });
            } else {
                sessionNotFound(packetId, sessionId);
            }
        } else {
            in.setSession(si.session);
            PacketDeliveryTask task = new PacketDeliveryTask(this, in, packetId, packetType, si);
            si.submitTask(task);
        }
    }

    private void readInitPacket(TransferInputStream in, int packetId, int sessionId, SessionInfo si) {
        try {
            SessionInit packet = SessionInit.decoder.decode(in, 0);
            ConnectionInfo ci = packet.ci;
            String baseDir = tcpServer.getBaseDir();
            if (baseDir == null) {
                baseDir = SysProperties.getBaseDirSilently();
            }
            // 强制使用服务器端的基目录
            if (baseDir != null) {
                ci.setBaseDir(baseDir);
            }
            Session session = createSession(ci, sessionId, si);
            session.setProtocolVersion(packet.clientVersion);
            in.setSession(session);

            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            SessionInitAck ack = new SessionInitAck(packet.clientVersion, session.isAutoCommit(),
                    session.getTargetNodes(), session.getRunMode(), session.isInvalid());
            ack.encode(out, packet.clientVersion);
            out.flush();
        } catch (Throwable e) {
            si.remove();
            logger.error("Failed to create session, packetId: " + packetId + ", sessionId: " + sessionId, e);
            sendError(null, packetId, e);
        }
    }

    private Session createSession(ConnectionInfo ci, int sessionId, SessionInfo si) {
        Session session = ci.createSession();
        // 在复制模式和sharding模式下，客户端可以从任何一个节点接入，
        // 如果接入节点不是客户端想要访问的数据库的所在节点，就会给客户端返回数据库的所有节点，
        // 此时，这样的session就是无效的，客户端会自动重定向到正确的节点。
        if (session.isValid()) {
            // 每个sessionId对应一个SessionInfo，每个调度器可以负责多个SessionInfo， 但是一个SessionInfo只能由一个调度器负责。
            // sessions这个字段并没有考虑放到调度器中，这样做的话光有sessionId作为key是不够的，
            // 还需要当前连接做限定，因为每个连接可以接入多个客户端session，不同连接中的sessionId是可以相同的，
            // 把sessions这个字段放在连接实例中可以减少并发访问的冲突。
            si.session = (ServerSession) session;
            sessions.put(sessionId, si);
        } else {
            si.remove();
        }
        return session;
    }

    private void sessionNotFound(int packetId, int sessionId) {
        String msg = "Server session not found, maybe closed or timeout. client session id: " + sessionId;
        RuntimeException e = new RuntimeException(msg);
        // logger.warn(msg, e); //打印错误堆栈不是很大必要
        logger.warn(msg);
        sendError(null, packetId, e);
    }

    public void closeSession(int packetId, int sessionId) {
        SessionInfo si = sessions.get(sessionId);
        if (si != null) {
            closeSession(si);
        } else {
            sessionNotFound(packetId, sessionId);
        }
    }

    void closeSession(SessionInfo si) {
        try {
            si.session.prepareStatement("ROLLBACK", -1).executeUpdate();
            si.session.close();
        } catch (Exception e) {
            logger.error("Failed to close session", e);
        } finally {
            si.remove();
            sessions.remove(si.sessionId);
        }
    }

    @Override
    public void close() {
        super.close();
        for (SessionInfo si : sessions.values()) {
            closeSession(si);
        }
        sessions.clear();
        cache.close();
        lobCache = null;
    }

    private static int getStatus(Session session) {
        if (session.isClosed()) {
            return Session.STATUS_CLOSED;
        } else if (session.isRunModeChanged()) {
            return Session.STATUS_RUN_MODE_CHANGED;
        } else {
            return Session.STATUS_OK;
        }
    }

    public void sendResponse(PacketDeliveryTask task, Packet packet) {
        ServerSession session = task.session;
        try {
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(task.packetId, getStatus(session));
            if (session.isRunModeChanged()) {
                out.writeInt(task.sessionId).writeString(session.getNewTargetNodes());
            }
            packet.encode(out, session.getProtocolVersion());
            out.flush();
        } catch (Exception e) {
            sendError(session, task.packetId, e);
        }
    }

    public void addCache(Integer k, AutoCloseable v) {
        cache.put(k, v);
    }

    public AutoCloseable getCache(Integer k) {
        return cache.get(k);
    }

    public AutoCloseable removeCache(Integer k, boolean ifAvailable) {
        return cache.remove(k, ifAvailable);
    }

    public LobCache getLobCache() {
        if (lobCache == null) {
            lobCache = new LobCache();
        }
        return lobCache;
    }
}
