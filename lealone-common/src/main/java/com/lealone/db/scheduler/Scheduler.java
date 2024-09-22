/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import com.lealone.common.logging.Logger;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.async.AsyncTaskHandler;
import com.lealone.db.session.Session;
import com.lealone.server.ProtocolServer;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.page.PageOperation;
import com.lealone.transaction.PendingTransaction;

public interface Scheduler extends AsyncTaskHandler, Runnable, SchedulerListener.Factory {

    int getId();

    String getName();

    Logger getLogger();

    long getLoad();

    SchedulerThread getThread();

    SchedulerFactory getSchedulerFactory();

    void setSchedulerFactory(SchedulerFactory schedulerFactory);

    void start();

    void stop();

    boolean isStarted();

    boolean isStopped();

    void addSession(Session session);

    void removeSession(Session session);

    DataBufferFactory getDataBufferFactory();

    Object getNetEventLoop();

    Selector getSelector();

    void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel);

    void accept(SelectionKey key);

    void addSessionInitTask(Object task);

    void addSessionInfo(Object si);

    void removeSessionInfo(Object si);

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

    Session getCurrentSession();

    void setCurrentSession(Session currentSession);

    void executeNextStatement();

    boolean yieldIfNeeded(PreparedSQLStatement current);

    void wakeUp();

    default boolean isEmbedded() {
        return false;
    }
}
