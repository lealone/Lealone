/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ExpiringMap;
import org.lealone.common.util.Pair;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.net.TransferConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.net.WritableChannel;
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
    private final Scheduler scheduler;

    public TcpServerConnection(TcpServer tcpServer, WritableChannel writableChannel, boolean isServer,
            Scheduler scheduler) {
        super(writableChannel, isServer);
        this.tcpServer = tcpServer;
        this.scheduler = scheduler;
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
                // Scheduler scheduler = ScheduleService.getSchedulerForSession();
                // scheduler.register(this);
                scheduler.handle(() -> readInitPacket(in, packetId, sessionId, scheduler));
            } else {
                sessionNotFound(packetId, sessionId);
            }
        } else {
            in.setSession(si.getSession());
            PacketDeliveryTask task = new PacketDeliveryTask(this, in, packetId, packetType, si);
            si.submitTask(task);
        }
    }

    private void readInitPacket(TransferInputStream in, int packetId, int sessionId, Scheduler scheduler) {
        SessionInit packet;
        try {
            packet = SessionInit.decoder.decode(in, 0);
        } catch (Throwable e) {
            logger.error("Failed to readInitPacket, packetId: " + packetId + ", sessionId: " + sessionId, e);
            sendError(null, packetId, e);
            return;
        } finally {
            in.closeInputStream();
        }

        try {
            ServerSession session = createSession(packet.ci, sessionId, scheduler);
            scheduler.validateUserAndPassword(true);
            session.setProtocolVersion(packet.clientVersion);
            sendSessionInitAck(packet, packetId, session);
        } catch (Throwable e) {
            if (DbException.convert(e).getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                scheduler.validateUserAndPassword(false);
            }
            SessionInfo si = sessions.get(sessionId);
            if (si != null) {
                closeSession(si);
            }
            logger.error("Failed to create session, sessionId: " + sessionId, e);
            sendError(null, packetId, e);
        }
    }

    private ServerSession createSession(ConnectionInfo ci, int sessionId, Scheduler scheduler) {
        ServerSession session = (ServerSession) ci.createSession();

        // 在复制模式和sharding模式下，客户端可以从任何一个节点接入，
        // 如果接入节点不是客户端想要访问的数据库的所在节点，就会给客户端返回数据库的所有节点，
        // 此时，这样的session就是无效的，客户端会自动重定向到正确的节点。
        if (session.isValid()) {
            // 每个sessionId对应一个SessionInfo，每个调度器可以负责多个SessionInfo， 但是一个SessionInfo只能由一个调度器负责。
            // sessions这个字段并没有考虑放到调度器中，这样做的话光有sessionId作为key是不够的，
            // 还需要当前连接做限定，因为每个连接可以接入多个客户端session，不同连接中的sessionId是可以相同的，
            // 把sessions这个字段放在连接实例中可以减少并发访问的冲突。
            session.setTransactionListener(scheduler);
            session.setCache(new ExpiringMap<>(ScheduleService.getScheduler(), tcpServer.getSessionTimeout(),
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
                    }));
            SessionInfo si = new SessionInfo(scheduler, this, session, sessionId, tcpServer.getSessionTimeout());
            session.setSessionInfo(si);
            scheduler.addSessionInfo(si);
            sessions.put(sessionId, si);
        }
        return session;
    }

    private void sendSessionInitAck(SessionInit packet, int packetId, ServerSession session) throws Exception {
        TransferOutputStream out = createTransferOutputStream(session);
        out.writeResponseHeader(packetId, Session.STATUS_OK);
        SessionInitAck ack = new SessionInitAck(packet.clientVersion, session.isAutoCommit(), session.getTargetNodes(),
                session.getRunMode(), session.isInvalid());
        ack.encode(out, packet.clientVersion);
        out.flush();
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
            ServerSession s = si.getSession();
            // 执行SHUTDOWN IMMEDIATELY时会模拟PowerOff，此时不必再执行后续操作
            if (!s.getDatabase().isPowerOff()) {
                s.rollback();
                s.close();
            }
        } catch (Exception e) {
            logger.error("Failed to close session", e);
        } finally {
            si.remove();
            sessions.remove(si.getSessionId());
        }
    }

    @Override
    public void close() {
        super.close();
        for (SessionInfo si : sessions.values()) {
            closeSession(si);
        }
        sessions.clear();
    }

    private static int getStatus(Session session) {
        if (session.isClosed()) {
            return Session.STATUS_CLOSED;
        } else if (session.isRunModeChanged()) {
            return Session.STATUS_RUN_MODE_CHANGED;
        } else if (session.getReplicationName() != null) {
            return Session.STATUS_REPLICATING;
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
}
