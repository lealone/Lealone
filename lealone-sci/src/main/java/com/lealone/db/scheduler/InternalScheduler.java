/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import com.lealone.db.DataBuffer;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionInfo;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.page.PageOperation;
import com.lealone.transaction.PendingTransaction;

public interface InternalScheduler extends Scheduler, SchedulerListener.Factory {

    @Override
    InternalSession getCurrentSession();

    @Override
    void setCurrentSession(Session currentSession);

    void addSession(InternalSession session);

    void removeSession(InternalSession session);

    void addSessionInitTask(Object task);

    void addSessionInfo(SessionInfo si);

    void removeSessionInfo(SessionInfo si);

    void validateSession(boolean isUserAndPasswordCorrect);

    void addPendingTransaction(PendingTransaction pt);

    PendingTransaction getPendingTransaction();

    void setFsyncDisabled(boolean fsyncDisabled);

    boolean isFsyncDisabled();

    void setFsyncingFileStorage(FileStorage fsyncingFileStorage);

    FileStorage getFsyncingFileStorage();

    void handlePageOperation(PageOperation po);

    void addWaitingScheduler(Scheduler scheduler);

    void wakeUpWaitingSchedulers();

    void wakeUpWaitingSchedulers(boolean reset);

    boolean yieldIfNeeded(PreparedSQLStatement current);

    DataBuffer getLogBuffer();

    DataBuffer getInputBuffer();

    DataBuffer getOutputBuffer();
}
