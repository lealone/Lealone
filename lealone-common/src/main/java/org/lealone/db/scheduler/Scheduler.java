/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.lealone.common.logging.Logger;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.TransactionHandler;
import org.lealone.transaction.TransactionListener;

public interface Scheduler extends PageOperationHandler, SQLStatementExecutor, AsyncTaskHandler,
        TransactionListener, TransactionHandler, Runnable {

    int getId();

    String getName();

    long getLoad();

    void start();

    void stop();

    boolean isStarted();

    boolean isStopped();

    SchedulerThread getThread();

    @Override
    DataBufferFactory getDataBufferFactory();

    Selector getSelector();

    void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel);

    void register(Object conn);

    default void registerConnectOperation(SocketChannel channel, Object attachment)
            throws ClosedChannelException {
    }

    Logger getLogger();

    SchedulerFactory getSchedulerFactory();

    void setSchedulerFactory(SchedulerFactory schedulerFactory);

    void addSessionInitTask(ISessionInitTask task);

    void addSessionInfo(ISessionInfo si);

    void addSession(Session session, int databaseId);

    void removeSessionInfo(ISessionInfo si);

    void validateSession(boolean isUserAndPasswordCorrect);

    @Override
    void wakeUp();

    void submitTask(Session session, AsyncTask task);
}
