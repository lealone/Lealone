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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.sql.PreparedSQLStatement;

public class SessionInfo implements ServerSession.TimeoutListener {

    private static final Logger logger = LoggerFactory.getLogger(SessionInfo.class);

    // taskQueue中的命令统一由scheduler调度执行
    private final ConcurrentLinkedQueue<AsyncTask> taskQueue = new ConcurrentLinkedQueue<>();
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
        scheduler.addSessionInfo(this);
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

    public void submitTask(AsyncTask task) {
        updateLastActiveTime();
        taskQueue.add(task);
        scheduler.wakeUp();
    }

    public void submitYieldableCommand(int packetId, PreparedSQLStatement stmt,
            PreparedSQLStatement.Yieldable<?> yieldable) {
        YieldableCommand yieldableCommand = new YieldableCommand(packetId, stmt, yieldable, session, sessionId);
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
                    + conn.getWritableChannel().getHost() + ", port: " + conn.getWritableChannel().getPort());
        }
    }

    void runSessionTasks() {
        if (session.canExecuteNextCommand()) {
            AsyncTask task = taskQueue.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run async session task: " + task + ", session id: " + sessionId, e);
                }
                // 执行Update或Query包的解析任务时会通过submitYieldableCommand设置
                if (session.getYieldableCommand() != null)
                    break;
                task = taskQueue.poll();
            }
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
