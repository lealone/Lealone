/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;

public class InternalDelegatedSession extends DelegatedSession implements InternalSession {

    protected InternalSession session;

    public InternalDelegatedSession() {
    }

    public InternalDelegatedSession(Session session) {
        setSession(session);
    }

    @Override
    public SessionStatus getStatus() {
        return session.getStatus();
    }

    @Override
    public void setStatus(SessionStatus sessionStatus) {
        session.setStatus(sessionStatus);
    }

    @Override
    public void setSession(Session session) {
        this.session = (InternalSession) session;
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public InternalScheduler getScheduler() {
        return session.getScheduler();
    }

    @Override
    public void setScheduler(InternalScheduler scheduler) {
        session.setScheduler(scheduler);
    }

    @Override
    public void setYieldableCommand(YieldableCommand yieldableCommand) {
        session.setYieldableCommand(yieldableCommand);
    }

    @Override
    public YieldableCommand getYieldableCommand() {
        return session.getYieldableCommand();
    }

    @Override
    public YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener) {
        return session.getYieldableCommand(checkTimeout, timeoutListener);
    }

    @Override
    public void init() {
        session.init();
    }

    @Override
    public boolean isBio() {
        return session.isBio();
    }

    @Override
    public void setSessionInfo(SessionInfo si) {
        session.setSessionInfo(si);
    }

    @Override
    public SessionInfo getSessionInfo() {
        return session.getSessionInfo();
    }
}
