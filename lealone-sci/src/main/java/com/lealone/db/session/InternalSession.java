/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.lock.Lock;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;
import com.lealone.transaction.Transaction;

public interface InternalSession extends Session {

    public default SessionStatus getStatus() {
        return SessionStatus.TRANSACTION_NOT_START;
    }

    public default void setStatus(SessionStatus sessionStatus) {
    }

    default void setLockedBy(SessionStatus sessionStatus, Transaction lockedBy, Object lockedObject) {
    }

    default Transaction getTransaction() {
        return null;
    }

    default void addLock(Lock lock) {
    }

    default void removeLock(Lock lock) {
    }

    default void addWaitingScheduler(Scheduler scheduler) {
    }

    default void wakeUpWaitingSchedulers() {
    }

    default boolean compareAndSet(SessionStatus expect, SessionStatus update) {
        return false;
    }

    default boolean isQueryCommand() {
        return false;
    }

    default boolean isUndoLogEnabled() {
        return true;
    }

    default boolean isRedoLogEnabled() {
        return true;
    }

    void init();

    @Override
    InternalScheduler getScheduler();

    void setScheduler(InternalScheduler scheduler);

    void setYieldableCommand(YieldableCommand yieldableCommand);

    YieldableCommand getYieldableCommand();

    YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener);

    public static interface TimeoutListener {
        void onTimeout(YieldableCommand c, Throwable e);
    }

    @Override
    void setSessionInfo(SessionInfo si);

    @Override
    SessionInfo getSessionInfo();
}
