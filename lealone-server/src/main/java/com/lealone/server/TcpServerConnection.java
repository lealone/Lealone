/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import java.io.IOException;
import java.util.HashMap;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;
import com.lealone.db.util.ExpiringMap;
import com.lealone.net.TransferInputStream;
import com.lealone.net.WritableChannel;
import com.lealone.server.handler.PacketHandler;
import com.lealone.server.handler.PacketHandlers;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketDecoders;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.session.SessionInit;
import com.lealone.server.protocol.session.SessionInitAck;
import com.lealone.server.scheduler.PacketHandleTask;
import com.lealone.server.scheduler.ServerSessionInfo;
import com.lealone.server.scheduler.SessionInitTask;

/**
 * 这里只处理客户端通过TCP连到服务器端后的协议，可以在一个TCP连接中打开多个session
 * 
 */
// 注意: 以下代码中出现的sessionId都表示客户端session的id，
// 调用createSession创建的是服务器端的session，这个session的id有可能跟客户端session的id不一样，
// 但是可以把客户端session的id跟服务器端的session做一个影射，这样两端的session就对上了。
//
// 每个TcpServerConnection实例对应一个Scheduler，也就是只会有一个调度服务线程执行它的方法
public class TcpServerConnection extends AsyncServerConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpServerConnection.class);

    // 每个sessionId对应一个专有的SessionInfo，
    // 所有与这个sessionId相关的命令请求都先放到SessionInfo中的队列，
    // 然后由调度器根据优先级从多个队列中依次取出执行。
    private final HashMap<Integer, ServerSessionInfo> sessions = new HashMap<>();
    private final TcpServer tcpServer;

    public TcpServerConnection(TcpServer tcpServer, WritableChannel writableChannel,
            Scheduler scheduler) {
        super(writableChannel, scheduler);
        this.tcpServer = tcpServer;
    }

    @Override
    public int getSessionCount() {
        return sessions.size();
    }

    @Override
    public void handleException(Exception e) {
        tcpServer.removeConnection(this);
    }

    @Override
    protected void handleRequest(TransferInputStream in, int packetId, int packetType)
            throws IOException {
        // 这里的sessionId是客户端session的id，每个数据包都会带这个字段
        int sessionId = in.readInt();
        ServerSessionInfo si = sessions.get(sessionId);
        if (si == null) {
            if (packetType == PacketType.SESSION_INIT.value) {
                readInitPacket(in, packetId, sessionId);
            } else {
                sessionNotFound(packetId, sessionId);
            }
        } else {
            ServerSession session = si.getSession();
            in.setSession(session);
            PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
            if (decoder != null) {
                Packet packet = decoder.decode(in, session.getProtocolVersion());
                @SuppressWarnings("unchecked")
                PacketHandler<Packet> handler = PacketHandlers.getHandler(packetType);
                PacketHandleTask task = new PacketHandleTask(packetId, si, packet, handler, this);
                si.submitTask(task, true);
            } else {
                logger.warn("Unknow packet type: {}", packetType);
            }
        }
        // 在父类中已经确保调用TransferInputStream.close
        // 所以在这里不用做任何处理
    }

    private void readInitPacket(TransferInputStream in, int packetId, int sessionId) {
        SessionInit packet;
        try {
            packet = SessionInit.decoder.decode(in, 0);
        } catch (Throwable e) {
            logger.error("Failed to readInitPacket, packetId: {}, sessionId: {}", packetId, sessionId,
                    e);
            sendError(null, packetId, e);
            return;
        }

        SessionInitTask task = new SessionInitTask(this, packet, packetId, sessionId);
        // 在事件循环中直接执行如果又需要建立新session(remote page的场景)会遇到麻烦，事件循环不能嵌套
        // if (scheduler.canHandleNextSessionInitTask()) {
        // // 直接处理，如果完成了就不需要加入Scheduler的队列
        // if (task.run())
        // return;
        // }
        scheduler.addSessionInitTask(task);
    }

    public boolean createSession(SessionInit packet, int packetId, int sessionId) {
        try {
            ServerSession session = (ServerSession) packet.ci.createSession();
            if (session == null) {
                return false;
            }
            addSession(session, sessionId);
            scheduler.validateSession(true);
            session.setProtocolVersion(packet.clientVersion);
            sendSessionInitAck(packet, packetId, session);
        } catch (Throwable e) {
            if (DbException.convert(e).getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                scheduler.validateSession(false);
            }
            ServerSessionInfo si = sessions.get(sessionId);
            if (si != null) {
                closeSession(si);
            }
            logger.error("Failed to create session, sessionId: " + sessionId, e);
            sendError(null, packetId, e);
        }
        return true;
    }

    private void addSession(ServerSession session, int sessionId) {
        // 在复制模式和sharding模式下，客户端可以从任何一个节点接入，
        // 如果接入节点不是客户端想要访问的数据库的所在节点，就会给客户端返回数据库的所有节点，
        // 此时，这样的session就是无效的，客户端会自动重定向到正确的节点。
        if (session.isValid()) {
            // 每个sessionId对应一个SessionInfo，每个调度器可以负责多个SessionInfo， 但是一个SessionInfo只能由一个调度器负责。
            // sessions这个字段并没有考虑放到调度器中，这样做的话光有sessionId作为key是不够的，
            // 还需要当前连接做限定，因为每个连接可以接入多个客户端session，不同连接中的sessionId是可以相同的，
            // 把sessions这个字段放在连接实例中可以减少并发访问的冲突。
            session.setScheduler(scheduler);
            session.setCache(
                    new ExpiringMap<>(scheduler, tcpServer.getSessionTimeout(), true, cObject -> {
                        try {
                            cObject.value.close();
                        } catch (Exception e) {
                            logger.warn(e.getMessage());
                        }
                        return null;
                    }));
            ServerSessionInfo si = new ServerSessionInfo(scheduler, this, session, sessionId,
                    tcpServer.getSessionTimeout());
            sessions.put(sessionId, si);
            scheduler.addSessionInfo(si);
        }
    }

    private void sendSessionInitAck(SessionInit packet, int packetId, ServerSession session)
            throws Exception {
        out.writeResponseHeader(session, packetId, Session.STATUS_OK);
        SessionInitAck ack = new SessionInitAck(packet.clientVersion, session.isAutoCommit(),
                session.getTargetNodes(), session.getRunMode(), session.isInvalid(), 0);
        ack.encode(out, packet.clientVersion);
        out.flush();
    }

    private void sessionNotFound(int packetId, int sessionId) {
        String msg = "Server session not found, maybe closed or timeout. client session id: "
                + sessionId;
        RuntimeException e = new RuntimeException(msg);
        // logger.warn(msg, e); //打印错误堆栈不是很大必要
        logger.warn(msg);
        sendError(null, packetId, e);
    }

    public void closeSession(int packetId, int sessionId) {
        ServerSessionInfo si = sessions.get(sessionId);
        if (si != null) {
            closeSession(si);
        } else {
            sessionNotFound(packetId, sessionId);
        }
    }

    @Override
    public void closeSession(ServerSessionInfo si) {
        closeSession(si, false);
    }

    private void closeSession(ServerSessionInfo si, boolean isForLoop) {
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
            if (!isForLoop) // 在循环中不能删除元素，否则会有并发更新异常
                sessions.remove(si.getSessionId());
        }
    }

    @Override
    public void close() {
        if (isClosed())
            return;
        in.close();
        // out不需要关闭
        super.close();
        for (ServerSessionInfo si : sessions.values()) {
            closeSession(si, true);
        }
        sessions.clear();
    }

    private static int getStatus(ServerSession session) {
        if (session.isClosed()) {
            return Session.STATUS_CLOSED;
        } else {
            return Session.STATUS_OK;
        }
    }

    public void sendResponse(PacketHandleTask task, Packet packet) {
        ServerSession session = task.session;
        try {
            out.writeResponseHeader(session, task.packetId, getStatus(session));
            packet.encode(out, session.getProtocolVersion());
            out.flush();
        } catch (Exception e) {
            sendError(session, task.packetId, e);
        }
    }
}
