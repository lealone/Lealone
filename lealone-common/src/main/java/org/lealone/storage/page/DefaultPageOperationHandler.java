/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.async.AsyncResult;

public class DefaultPageOperationHandler extends PageOperationHandlerBase
        implements Runnable, PageOperation.Listener<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageOperationHandler.class);
    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stopped;
    private volatile boolean waiting;

    public DefaultPageOperationHandler(int id, int waitingQueueSize, Map<String, String> config) {
        super(id, DefaultPageOperationHandler.class.getSimpleName() + "-" + id, waitingQueueSize);
        // 默认100毫秒
        loopInterval = MapUtils.getLong(config, "page_operation_handler_loop_interval", 100);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public void startHandler() {
        if (stopped)
            return;
        stopped = false;
        ShutdownHookUtils.addShutdownHook(getName(), () -> {
            stopHandler();
        });
        start();
    }

    public void stopHandler() {
        stopped = true;
        wakeUp();
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

    // 以下使用同步方式执行
    private volatile RuntimeException e;
    private volatile Object result;

    @Override
    public Object await() {
        e = null;
        result = null;
        while (result == null || e == null) {
            runPageOperationTasks();
            doAwait();
        }
        if (e != null)
            throw e;
        return result;
    }

    @Override
    public void handle(AsyncResult<Object> ar) {
        if (ar.isSucceeded())
            result = ar.getResult();
        else
            e = new RuntimeException(ar.getCause());
        wakeUp();
    }
}
