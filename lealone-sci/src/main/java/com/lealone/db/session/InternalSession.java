/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;
import com.lealone.storage.page.IPage;
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

    default void addLock(Object lock) {
    }

    default void removeLock(Object lock) {
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

    default boolean isForUpdate() {
        return false;
    }

    default boolean isUndoLogEnabled() {
        return true;
    }

    default void addPageReference(Object ref) {
    }

    default void addPageReference(Object oldRef, Object lRef, Object rRef) {
    }

    default boolean containsPageReference(Object ref) {
        return false;
    }

    default void addDirtyPage(IPage page) {
        addDirtyPage(null, page);
    }

    default void addDirtyPage(IPage old, IPage page) {
    }

    default void markDirtyPages() {
    }

    InternalScheduler getScheduler();

    void setScheduler(InternalScheduler scheduler);

    void setYieldableCommand(YieldableCommand yieldableCommand);

    YieldableCommand getYieldableCommand();

    YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener);

    public static interface TimeoutListener {
        void onTimeout(YieldableCommand c, Throwable e);
    }

    void init();

    void setSessionInfo(SessionInfo si);

    SessionInfo getSessionInfo();
}
