/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.sql.PreparedSQLStatement;

public class SessionInfo implements ServerSession.TimeoutListener {

    private static final Logger logger = LoggerFactory.getLogger(SessionInfo.class);

    // taskQueue中的命令统一由scheduler调度执行
    private final Queue<AsyncTask> taskQueue = new LinkedList<>();
    private final Scheduler scheduler;
    private final TcpServerConnection conn;

    private final ServerSession session;
    private final int sessionId; // 客户端的sessionId
    private final int sessionTimeout;

    private long lastActiveTime;

    SessionInfo(Scheduler scheduler, TcpServerConnection conn, ServerSession session, int sessionId,
            int sessionTimeout) {
        this.scheduler = scheduler;
        this.conn = conn;
        this.session = session;
        this.sessionId = sessionId;
        this.sessionTimeout = sessionTimeout;
        updateLastActiveTime();
    }

    SessionInfo copy(ServerSession session) {
        return new SessionInfo(this.scheduler, this.conn, session, this.sessionId, this.sessionTimeout);
    }

    private void updateLastActiveTime() {
        lastActiveTime = System.currentTimeMillis();
    }

    ServerSession getSession() {
        return session;
    }

    int getSessionId() {
        return sessionId;
    }

    public void submitTask(PacketHandleTask task) {
        updateLastActiveTime();
        taskQueue.add(task);
    }

    public void submitTasks(AsyncTask... tasks) {
        updateLastActiveTime();
        taskQueue.addAll(Arrays.asList(tasks));
    }

    public void submitYieldableCommand(int packetId, PreparedSQLStatement.Yieldable<?> yieldable) {
        yieldable.setExecutor(scheduler);
        YieldableCommand yieldableCommand = new YieldableCommand(packetId, yieldable, sessionId);
        session.setYieldableCommand(yieldableCommand);
        // 执行此方法的当前线程就是scheduler，所以不用唤醒scheduler
    }

    void remove() {
        scheduler.removeSessionInfo(this);
    }

    void checkSessionTimeout(long currentTime) {
        if (sessionTimeout <= 0)
            return;
        if (lastActiveTime + sessionTimeout < currentTime) {
            conn.closeSession(this);
            logger.warn("Client session timeout, session id: " + sessionId + ", host: "
                    + conn.getWritableChannel().getHost() + ", port: "
                    + conn.getWritableChannel().getPort());
        }
    }

    private void runTask(AsyncTask task) {
        try {
            task.run();
        } catch (Throwable e) {
            logger.warn("Failed to run async session task: " + task + ", session id: " + sessionId, e);
        }
    }

    void runSessionTasks() {
        // 在同一session中，只有前面一条SQL执行完后才可以执行下一条
        if (session.getYieldableCommand() != null)
            return;
        AsyncTask task = taskQueue.poll();
        while (task != null) {
            runTask(task);
            // 执行Update或Query包的解析任务时会通过submitYieldableCommand设置
            if (session.getYieldableCommand() != null)
                break;
            task = taskQueue.poll();
        }
    }

    YieldableCommand getYieldableCommand(boolean checkTimeout) {
        return session.getYieldableCommand(checkTimeout, this);
    }

    void sendError(int packetId, Throwable e) {
        conn.sendError(session, packetId, e);
    }

    @Override
    public void onTimeout(YieldableCommand c, Throwable e) {
        sendError(c.getPacketId(), e);
    }
}
