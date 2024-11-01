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

public interface Scheduler extends AsyncTaskHandler, Runnable {

    int getId();

    String getName();

    Logger getLogger();

    long getLoad();

    default boolean isBusy() {
        return false;
    }

    SchedulerThread getThread();

    SchedulerFactory getSchedulerFactory();

    void setSchedulerFactory(SchedulerFactory schedulerFactory);

    void start();

    void stop();

    boolean isStarted();

    boolean isStopped();

    void wakeUp();

    Session getCurrentSession();

    void setCurrentSession(Session currentSession);

    void executeNextStatement();

    DataBufferFactory getDataBufferFactory();

    Object getNetEventLoop();

    Selector getSelector();

    void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel);

    void accept(SelectionKey key);

    void addSession(Session session);

    void removeSession(Session session);

}
