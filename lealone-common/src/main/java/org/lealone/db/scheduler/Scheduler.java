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
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.async.PendingTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.TransactionHandler;
import org.lealone.transaction.TransactionListener;

public interface Scheduler extends PageOperationHandler, SQLStatementExecutor, AsyncTaskHandler,
        TransactionListener, TransactionHandler, Runnable, PageOperation.ListenerFactory<Object> {

    int getId();

    String getName();

    long getLoad();

    void start();

    void stop();

    boolean isStarted();

    boolean isStopped();

    SchedulerThread getThread();

    DataBufferFactory getDataBufferFactory();

    default Object getNetEventLoop() {
        return null;
    }

    Selector getSelector();

    void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel);

    void register(Object conn);

    default void registerConnectOperation(SocketChannel channel, Object attachment)
            throws ClosedChannelException {
    }

    Logger getLogger();

    SchedulerFactory getSchedulerFactory();

    void setSchedulerFactory(SchedulerFactory schedulerFactory);

    void addSessionInitTask(Object task);

    void addSessionInfo(Object si);

    void addSession(Session session, int databaseId);

    default void removeSession(Session session) {
    }

    void removeSessionInfo(Object si);

    void validateSession(boolean isUserAndPasswordCorrect);

    @Override
    void wakeUp();

    void addPendingTaskHandler(PendingTaskHandler hdandler);

    void removePendingTaskHandler(PendingTaskHandler hdandler);
}
