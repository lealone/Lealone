/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;

public abstract class InternalSessionBase extends SessionBase implements InternalSession {

    protected InternalScheduler scheduler;

    @Override
    public InternalScheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(InternalScheduler scheduler) {
        this.scheduler = scheduler;
    }

    protected YieldableCommand yieldableCommand;

    @Override
    public void setYieldableCommand(YieldableCommand yieldableCommand) {
        this.yieldableCommand = yieldableCommand;
    }

    @Override
    public YieldableCommand getYieldableCommand() {
        return yieldableCommand;
    }

    @Override
    public YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener) {
        return yieldableCommand;
    }

    @Override
    public void init() {
    }

    protected SessionInfo si;

    @Override
    public void setSessionInfo(SessionInfo si) {
        this.si = si;
    }

    @Override
    public SessionInfo getSessionInfo() {
        return si;
    }
}
