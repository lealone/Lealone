/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.scheduler;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableBase;
import com.lealone.db.link.LinkableList;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.ServerSession;
import com.lealone.server.AsyncServerConnection;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;

public class SessionInfo extends LinkableBase<SessionInfo> implements ServerSession.TimeoutListener {

    private static final Logger logger = LoggerFactory.getLogger(SessionInfo.class);

    private final Scheduler scheduler;
    private final AsyncServerConnection conn;

    private final ServerSession session;
    private final int sessionId; // 客户端的sessionId
    private final int sessionTimeout;

    private long lastActiveTime;

    // task统一由scheduler调度执行
    private final LinkableList<LinkableTask> tasks = new LinkableList<>();

    public SessionInfo(Scheduler scheduler, AsyncServerConnection conn, ServerSession session,
            int sessionId, int sessionTimeout) {
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

    public ServerSession getSession() {
        return session;
    }

    public int getSessionId() {
        return sessionId;
    }

    private void addTask(LinkableTask task) {
        tasks.add(task);
    }

    public void submitTask(LinkableTask task) {
        updateLastActiveTime();
        if (canHandleNextSessionTask()) // 如果可以直接处理下一个task就不必加到队列了
            runTask(task);
        else
            addTask(task);
    }

    public void submitTasks(LinkableTask... tasks) {
        updateLastActiveTime();
        for (LinkableTask task : tasks)
            addTask(task);
    }

    public void submitYieldableCommand(int packetId, PreparedSQLStatement.Yieldable<?> yieldable) {
        YieldableCommand yieldableCommand = new YieldableCommand(packetId, yieldable, sessionId);
        session.setYieldableCommand(yieldableCommand);
        // 执行此方法的当前线程就是scheduler，所以不用唤醒scheduler
    }

    public void remove() {
        scheduler.removeSessionInfo(this);
    }

    void checkSessionTimeout(long currentTime) {
        if (sessionTimeout <= 0)
            return;
        if (lastActiveTime + sessionTimeout < currentTime) {
            conn.closeSession(this);
            logger.warn("Client session timeout, session id: " + sessionId //
                    + ", host: " + conn.getWritableChannel().getHost() //
                    + ", port: " + conn.getWritableChannel().getPort());
        }
    }

    private void runTask(AsyncTask task) {
        ServerSession old = (ServerSession) scheduler.getCurrentSession();
        scheduler.setCurrentSession(session);
        try {
            task.run();
        } catch (Throwable e) {
            logger.warn("Failed to run async session task: " + task + ", session id: " + sessionId, e);
        } finally {
            scheduler.setCurrentSession(old);
        }
    }

    // 在同一session中，只有前面一条SQL执行完后才可以执行下一条
    private boolean canHandleNextSessionTask() {
        return session.canExecuteNextCommand();
    }

    void runSessionTasks() {
        // 只有当前语句执行完了才能执行下一条命令
        if (session.getYieldableCommand() != null) {
            return;
        }
        if (!tasks.isEmpty() && session.canExecuteNextCommand()) {
            LinkableTask task = tasks.getHead();
            while (task != null) {
                runTask(task);
                task = task.next;
                tasks.setHead(task);
                tasks.decrementSize();
                // 执行Update或Query包的解析任务时会通过submitYieldableCommand设置
                if (session.getYieldableCommand() != null)
                    break;
            }
            if (tasks.getHead() == null)
                tasks.setTail(null);
        }
    }

    YieldableCommand getYieldableCommand(boolean checkTimeout) {
        return session.getYieldableCommand(checkTimeout, this);
    }

    void sendError(int packetId, Throwable e) {
        // 如果session没有对应的connection不需要发送错误信息
        if (conn != null)
            conn.sendError(session, packetId, e);
        else
            logger.error("", e);
    }

    @Override
    public void onTimeout(YieldableCommand c, Throwable e) {
        sendError(c.getPacketId(), e);
    }

    boolean isMarkClosed() {
        if (session.isMarkClosed()) {
            if (conn != null) {
                conn.closeSession(this);
                if (conn.getSessionCount() == 0)
                    conn.close();
            }
            return true;
        }
        return false;
    }
}
