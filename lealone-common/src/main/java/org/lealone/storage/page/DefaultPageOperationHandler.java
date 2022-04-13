/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.page.PageOperation.PageOperationResult;

public class DefaultPageOperationHandler implements PageOperationHandler, Runnable, PageOperation.Listener<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageOperationHandler.class);
    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    private final ConcurrentLinkedQueue<PageOperation> tasks = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PageOperationHandler> waitingHandlers = new ConcurrentLinkedQueue<>();
    private final AtomicLong size = new AtomicLong();
    private final Semaphore haveWork = new Semaphore(1);
    private final String name;
    private final long loopInterval;
    private Thread thread;
    private boolean stopped;
    private volatile boolean waiting;

    public DefaultPageOperationHandler(int id, Map<String, String> config) {
        this(DefaultPageOperationHandler.class.getSimpleName() + "-" + id, config);
    }

    public DefaultPageOperationHandler(String name, Map<String, String> config) {
        this.name = name;
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "page_operation_handler_loop_interval", 100);
    }

    @Override
    public long getLoad() {
        return size.get();
    }

    @Override
    public void handlePageOperation(PageOperation task) {
        size.incrementAndGet();
        tasks.add(task);
        wakeUp();
    }

    @Override
    public void addWaitingHandler(PageOperationHandler handler) {
        waitingHandlers.add(handler);
    }

    @Override
    public void wakeUpWaitingHandlers() {
        if (!waitingHandlers.isEmpty()) {
            for (PageOperationHandler handler : waitingHandlers) {
                handler.wakeUp();
            }
            waitingHandlers.clear();
        }
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public void reset(boolean clearTasks) {
        thread = null;
        stopped = false;
        if (clearTasks) {
            size.set(0);
            tasks.clear();
        }
    }

    public void start() {
        if (thread != null)
            return;
        stopped = false;
        ShutdownHookUtils.addShutdownHook(name, () -> {
            stop();
        });
        thread = new Thread(this, name);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        stopped = true;
        thread = null;
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
            runTasks();
            doAwait();
        }
    }

    private void runTasks() {
        // 先peek，执行成功时再poll，严格保证每个PageOperation的执行顺序
        PageOperation task = tasks.peek();
        while (task != null) {
            try {
                PageOperationResult result = task.run(this);
                if (result == PageOperationResult.LOCKED) {
                    break;
                } else if (result == PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                logger.warn("Failed to run page operation: " + task, e);
            }
            size.decrementAndGet();
            tasks.poll();
            task = tasks.peek();
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
            runTasks();
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
