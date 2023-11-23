/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import org.lealone.common.logging.Logger;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.async.PendingTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.TransactionListener;

public interface Scheduler extends PageOperationHandler, SQLStatementExecutor, AsyncTaskHandler,
        TransactionListener, Runnable, PageOperation.ListenerFactory<Object> {

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

    void addSession(Session session, int databaseId);

    void removeSession(Session session);

    DataBufferFactory getDataBufferFactory();

    Object getNetEventLoop();

    Selector getSelector();

    void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel);

    void addSessionInitTask(Object task);

    void addSessionInfo(Object si);

    void removeSessionInfo(Object si);

    void validateSession(boolean isUserAndPasswordCorrect);

    @Override
    void wakeUp();

    void addPendingTaskHandler(PendingTaskHandler hdandler);

    void removePendingTaskHandler(PendingTaskHandler hdandler);

    void addPendingTransaction(PendingTransaction pt);

    PendingTransaction getPendingTransaction();
}
