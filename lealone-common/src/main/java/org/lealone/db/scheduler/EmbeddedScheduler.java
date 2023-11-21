/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.PluginManager;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.PendingTask;
import org.lealone.db.link.LinkableBase;
import org.lealone.db.link.LinkableList;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.TransactionListener;

public class EmbeddedScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedScheduler.class);
    private final Semaphore haveWork = new Semaphore(1);
    private volatile boolean waiting;

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();

    // --------------------- 以下字段用于实现 PageOperationHandler 接口 ---------------------

    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    // 外部线程和调度线程会并发访问这个队列
    private final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    private final AtomicLong pageOperationSize = new AtomicLong();
    private PageOperation lockedPageOperation;

    public static class SessionInfo extends LinkableBase<SessionInfo> {

        Session session;

        public SessionInfo(Session session) {
            this.session = session;
        }
    }

    private final LinkableList<SessionInfo> sessions = new LinkableList<>();

    public EmbeddedScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "EScheduleService-" + id, schedulerCount, config);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return sessions.size() + pageOperationSize.get();
    }

    @Override
    public void addSession(Session session, int databaseId) {
        sessions.add(new SessionInfo(session));
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
            runPendingTransactions();
            runMiscTasks();
            runSessionTasks();
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
            AsyncTask task = miscTasks.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + task, e);
                }
                task = miscTasks.poll();
            }
        }
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        SessionInfo si = sessions.getHead();
        while (si != null) {
            PendingTask pt = si.session.getPendingTask();
            while (pt != null) {
                if (!pt.isCompleted()) {
                    try {
                        pt.getTask().run();
                    } catch (Throwable e) {
                        logger.warn("Failed to run pending task: " + pt.getTask(), e);
                    }
                    pt.setCompleted(true);
                }
                pt = pt.getNext();
            }
            si = si.next;
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
            runPendingTransactions();
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

    private static SchedulerFactory defaultSchedulerFactory;

    public static void setDefaultSchedulerFactory(SchedulerFactory defaultSchedulerFactory) {
        EmbeddedScheduler.defaultSchedulerFactory = defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(Properties prop) {
        if (EmbeddedScheduler.defaultSchedulerFactory == null) {
            Map<String, String> config;
            if (prop != null)
                config = new CaseInsensitiveMap<>(prop);
            else
                config = new CaseInsensitiveMap<>();
            initDefaultSchedulerFactory(config);
        }
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(Map<String, String> config) {
        if (EmbeddedScheduler.defaultSchedulerFactory == null)
            initDefaultSchedulerFactory(config);
        return defaultSchedulerFactory;
    }

    public static synchronized SchedulerFactory initDefaultSchedulerFactory(Map<String, String> config) {
        SchedulerFactory schedulerFactory = EmbeddedScheduler.defaultSchedulerFactory;
        if (schedulerFactory == null) {
            String sf = MapUtils.getString(config, "scheduler_factory", null);
            if (sf != null) {
                schedulerFactory = PluginManager.getPlugin(SchedulerFactory.class, sf);
            } else {
                EmbeddedScheduler[] schedulers = createSchedulers(config);
                schedulerFactory = SchedulerFactory.create(config, schedulers);
            }
            if (!schedulerFactory.isInited())
                schedulerFactory.init(config);
            EmbeddedScheduler.defaultSchedulerFactory = schedulerFactory;
        }
        return schedulerFactory;
    }

    public static Scheduler getScheduler(ConnectionInfo ci) {
        Scheduler scheduler = ci.getScheduler();
        if (scheduler == null) {
            SchedulerFactory sf = EmbeddedScheduler.getDefaultSchedulerFactory(ci.getProperties());
            scheduler = sf.getScheduler();
            ci.setScheduler(scheduler);
            if (!sf.isStarted())
                sf.start();
        }
        return scheduler;
    }

    // 存放还没有给客户端发送响应结果的事务
    private final LinkableList<PendingTransaction> pendingTransactions = new LinkableList<>();

    // --------------------- 实现 TransactionHandler 接口 ---------------------

    // runPendingTransactions和addTransaction已经确保只有一个调度线程执行，所以是单线程安全的
    private void runPendingTransactions() {
        if (pendingTransactions.isEmpty())
            return;
        PendingTransaction pt = pendingTransactions.getHead();
        while (pt != null && pt.isSynced()) {
            if (!pt.isCompleted()) {
                try {
                    pt.getTransaction().asyncCommitComplete();
                } catch (Throwable e) {
                    if (logger != null)
                        logger.warn("Failed to run pending transaction: " + pt, e);
                }
            }
            pt = pt.getNext();
            pendingTransactions.decrementSize();
            pendingTransactions.setHead(pt);
        }
        if (pendingTransactions.getHead() == null)
            pendingTransactions.setTail(null);
    }

    @Override
    public void addTransaction(PendingTransaction pt) {
        pendingTransactions.add(pt);
    }

    @Override
    public PendingTransaction getTransaction() {
        return pendingTransactions.getHead();
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return null;
    }

    @Override
    public void addWaitingTransactionListener(TransactionListener listener) {
        addWaitingHandler((PageOperationHandler) listener);
    }

    @Override
    public void wakeUpWaitingTransactionListeners() {
        wakeUpWaitingHandlers();
    }
}
