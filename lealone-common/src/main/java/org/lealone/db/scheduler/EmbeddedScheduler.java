/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;
import org.lealone.transaction.PendingTransaction;

public class EmbeddedScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedScheduler.class);
    private final Semaphore haveWork = new Semaphore(1);
    private volatile boolean waiting;

    // 杂七杂八的任务，数量不多，执行完就删除
    private final CopyOnWriteArrayList<AsyncTask> miscTasks = new CopyOnWriteArrayList<>();

    // --------------------- 以下字段用于实现 PageOperationHandler 接口 ---------------------

    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    // 外部线程和调度线程会并发访问这个队列
    private final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    private final AtomicLong pageOperationSize = new AtomicLong();
    private PageOperation lockedPageOperation;

    public EmbeddedScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "EScheduleService-" + id, schedulerCount, config);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return pageOperationSize.get();
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
            runMiscTasks();
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

    private void runMiscTasks() {
        if (!miscTasks.isEmpty()) {
            Iterator<AsyncTask> iterator = miscTasks.iterator();
            while (iterator.hasNext()) {
                AsyncTask task = iterator.next();
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + task, e);
                }
                iterator.remove();
            }
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
        miscTasks.add(task);
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

    // --------------------- 实现 PageOperationHandler 接口 ---------------------

    @Override
    public void handlePageOperation(PageOperation po) {
        pageOperationSize.incrementAndGet();
        pageOperations.add(po);
        wakeUp();
    }

    private void runPageOperationTasks() {
        PageOperation po;
        // 先执行上一个被锁定的PageOperation，严格保证每个PageOperation的执行顺序
        if (lockedPageOperation != null) {
            po = lockedPageOperation;
            lockedPageOperation = null;
        } else {
            po = pageOperations.poll();
        }
        while (po != null) {
            try {
                PageOperationResult result = po.run(this);
                if (result == PageOperationResult.LOCKED) {
                    lockedPageOperation = po;
                    break;
                } else if (result == PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                getLogger().warn("Failed to run page operation: " + po, e);
            }
            pageOperationSize.decrementAndGet();
            po = pageOperations.poll();
        }
    }

    // --------------------- 创建所有的调度器 ---------------------

    public static EmbeddedScheduler[] createSchedulers(Map<String, String> config) {
        int schedulerCount = MapUtils.getSchedulerCount(config);
        EmbeddedScheduler[] schedulers = new EmbeddedScheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new EmbeddedScheduler(i, schedulerCount, config);
        }
        return schedulers;
    }
}
