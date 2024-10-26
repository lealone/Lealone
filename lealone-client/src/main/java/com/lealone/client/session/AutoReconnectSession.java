/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.session;

import com.lealone.db.ConnectionInfo;
import com.lealone.db.command.SQLCommand;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.db.session.DelegatedSession;
import com.lealone.db.session.Session;

public class AutoReconnectSession extends DelegatedSession {

    private final ConnectionInfo ci;

    AutoReconnectSession(ConnectionInfo ci, Session session) {
        this.ci = ci;
        this.session = session;
    }

    @Override
    public boolean isClosed() {
        return false; // 因为要自动重连了，所以总是返回false
    }

    private void reconnect() {
        // 重连的线程不是Scheduler时重置ConnectionInfo
        if (SchedulerThread.currentScheduler() == null) {
            ci.setSingleThreadCallback(false);
            ci.setScheduler(null);
        }
        ci.getSessionFactory().createSession(ci).onSuccess(s -> {
            AutoReconnectSession a = (AutoReconnectSession) s;
            session = a.session;
        }).get();
    }

    private void reconnectIfNeeded() {
        if (session.isClosed()) {
            reconnect();
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        reconnectIfNeeded();
        session.setAutoCommit(autoCommit);
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize, boolean prepared) {
        reconnectIfNeeded();
        return session.createSQLCommand(sql, fetchSize, prepared);
    }
}
