/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.transaction.PendingTransaction;

public class EmbeddedScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedScheduler.class);
    private final Semaphore haveWork = new Semaphore(1);
    private volatile boolean waiting;

    public EmbeddedScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "EmbeddedScheduleService-" + id, schedulerCount, config);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public void wakeUp() {
        if (waiting)
            haveWork.release(1);
    }

    @Override
    public void run() {
        while (!stopped) {
            runPageOperationTasks();
            doAwait();
        }
    }

    private void doAwait() {
        waiting = true;
        try {
            haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
            haveWork.drainPermits();
        } catch (InterruptedException e) {
            logger.warn("", e);
        } finally {
            waiting = false;
        }
    }

    @Override
    public void executeNextStatement() {
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        return false;
    }

    @Override
    public void handle(AsyncTask task) {
    }

    @Override
    public void operationUndo() {
    }

    @Override
    public void operationComplete() {
    }

    @Override
    public void addTransaction(PendingTransaction pt) {
    }

    @Override
    public PendingTransaction getTransaction() {
        return null;
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return null;
    }

    @Override
    public Selector getSelector() {
        return null;
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
    }

    @Override
    public void register(Object conn) {
    }

    @Override
    public void addSessionInitTask(ISessionInitTask task) {
    }

    @Override
    public void addSessionInfo(ISessionInfo si) {
    }

    @Override
    public void removeSessionInfo(ISessionInfo si) {

    }

    @Override
    public void validateSession(boolean isUserAndPasswordCorrect) {
    }

    @Override
    public void await() {
        for (;;) {
            if (syncCounter.get() < 1)
                break;
            runPageOperationTasks();
            if (syncCounter.get() < 1)
                break;
        }
        needWakeUp = true;
        if (syncException != null)
            throw syncException;
    }
}
