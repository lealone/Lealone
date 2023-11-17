/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.RunMode;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandlerBase;

public abstract class SchedulerBase extends PageOperationHandlerBase
        implements Scheduler, PageOperation.ListenerFactory<Object> {

    protected int id;
    protected String name;
    protected SchedulerFactory schedulerFactory;

    protected final long loopInterval;
    protected boolean daemon;
    protected boolean started;
    protected boolean stopped;
    protected Thread thread;

    public SchedulerBase(int id, String name, int schedulerCount, Map<String, String> config) {
        super(schedulerCount);
        this.id = id;
        this.name = name;
        this.daemon = RunMode.isEmbedded(config);
        // 默认100毫秒
        this.loopInterval = MapUtils.getLong(config, "scheduler_loop_interval", 100);
    }

    @Override
    public int getHandlerId() {
        return id;
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
    }

    @Override
    public void setSchedulerFactory(SchedulerFactory schedulerFactory) {
        this.schedulerFactory = schedulerFactory;
    }

    @Override
    public Thread getThread() {
        return thread;
    }

    @Override
    public synchronized void start() {
        if (started)
            return;
        thread = new Thread(this);
        thread.setName(name);
        thread.setDaemon(daemon);
        ShutdownHookUtils.addShutdownHook(getName(), () -> {
            stop();
        });
        thread.start();
        started = true;
        stopped = false;
    }

    @Override
    public synchronized void stop() {
        started = false;
        stopped = true;
        thread = null;
        wakeUp();
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    // --------------------- 实现 TransactionListener 接口，用同步方式执行 ---------------------

    protected AtomicInteger syncCounter;
    protected RuntimeException syncException;
    protected boolean needWakeUp = true;

    @Override
    public void setNeedWakeUp(boolean needWakeUp) {
        this.needWakeUp = needWakeUp;
    }

    @Override
    public int getListenerId() {
        return getHandlerId();
    }

    @Override
    public void beforeOperation() {
        syncException = null;
        syncCounter = new AtomicInteger(1);
    }

    @Override
    public void operationUndo() {
        syncCounter.decrementAndGet();
        if (needWakeUp)
            wakeUp();
    }

    @Override
    public void operationComplete() {
        syncCounter.decrementAndGet();
        if (needWakeUp)
            wakeUp();
    }

    @Override
    public void setException(RuntimeException e) {
        syncException = e;
    }

    @Override
    public RuntimeException getException() {
        return syncException;
    }

    @Override
    public void await() {
    }

    // protected abstract void runLoopTasks();

    @Override
    public Object addSession(Session session, Object parentSessionInfo) {
        throw DbException.getInternalError();
    }

    @Override
    public void removeSession(Object sessionInfo) {
        throw DbException.getInternalError();
    }

    // --------------------- 实现 PageOperation.ListenerFactory 接口 ---------------------

    @Override
    public PageOperation.Listener<Object> createListener() {
        return new PageOperation.Listener<Object>() {

            private Object result;

            @Override
            public void startListen() {
                beforeOperation();
            }

            @Override
            public void handle(AsyncResult<Object> ar) {
                if (ar.isSucceeded()) {
                    result = ar.getResult();
                } else {
                    setException(ar.getCause());
                }
                operationComplete();
            }

            @Override
            public Object await() {
                SchedulerBase.this.await();
                return result;
            }
        };
    }
}
