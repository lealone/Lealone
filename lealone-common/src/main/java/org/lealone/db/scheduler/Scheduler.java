/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import org.lealone.common.logging.Logger;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.page.PageOperation;
import org.lealone.transaction.PendingTransaction;

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

    Session getCurrentSession();

    void setCurrentSession(Session currentSession);

    void executeNextStatement();

    boolean yieldIfNeeded(PreparedSQLStatement current);

    void wakeUp();
}
