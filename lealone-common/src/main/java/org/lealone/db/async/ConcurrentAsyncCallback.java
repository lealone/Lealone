/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.scheduler.SchedulerThread;
import org.lealone.net.NetInputStream;

public class ConcurrentAsyncCallback<T> extends AsyncCallback<T> {

    private volatile boolean runEnd;
    private volatile AsyncHandler<AsyncResult<T>> completeHandler;
    private volatile AsyncHandler<T> successHandler;
    private volatile AsyncHandler<Throwable> failureHandler;
    private volatile AsyncResult<T> asyncResult;

    private final AtomicReference<LatchObject> latchObjectRef = new AtomicReference<>();

    private static class LatchObject {
        final CountDownLatch latch;

        public LatchObject(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    public ConcurrentAsyncCallback() {
    }

    @Override
    public void setDbException(DbException e, boolean cancel) {
        setAsyncResult(e);
        if (cancel)
            countDown();
    }

    @Override
    public final void run(NetInputStream in) {
        // 放在最前面，不能放在最后面，
        // 否则调用了countDown，但是在设置runEnd为true前，调用await的线程读到的是false就会抛异常
        runEnd = true;
        // if (asyncResult == null) {
        try {
            runInternal(in);
        } catch (Throwable t) {
            setAsyncResult(t);
        }
        // }
    }

    @Override
    protected T await(long timeoutMillis) {
        Scheduler scheduler = SchedulerThread.currentScheduler();
        if (scheduler != null)
            scheduler.executeNextStatement();
        if (latchObjectRef.compareAndSet(null, new LatchObject(new CountDownLatch(1)))) {
            CountDownLatch latch = latchObjectRef.get().latch;
            try {
                if (timeoutMillis > 0)
                    latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
                else
                    latch.await();
                if (asyncResult != null && asyncResult.isFailed())
                    throw DbException.convert(asyncResult.getCause());

                // 如果没有执行过run，抛出合适的异常
                if (!runEnd) {
                    handleTimeout();
                }
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            }
            if (asyncResult != null)
                return asyncResult.getResult();
            else
                return null;
        } else {
            if (asyncResult.isFailed())
                throw DbException.convert(asyncResult.getCause());
            else
                return asyncResult.getResult();
        }
    }

    @Override
    public synchronized Future<T> onSuccess(AsyncHandler<T> handler) {
        successHandler = handler;
        if (asyncResult != null && asyncResult.isSucceeded()) {
            handler.handle(asyncResult.getResult());
        }
        return this;
    }

    @Override
    public synchronized Future<T> onFailure(AsyncHandler<Throwable> handler) {
        failureHandler = handler;
        if (asyncResult != null && asyncResult.isFailed()) {
            handler.handle(asyncResult.getCause());
        }
        return this;
    }

    @Override
    public synchronized Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        completeHandler = handler;
        if (asyncResult != null) {
            handler.handle(asyncResult);
        }
        return this;
    }

    @Override
    public synchronized void setAsyncResult(AsyncResult<T> asyncResult) { // 复制集群场景可能会调用多次
        runEnd = true;
        this.asyncResult = asyncResult;
        try {
            if (completeHandler != null) {
                completeHandler.handle(asyncResult);
            }

            if (successHandler != null && asyncResult != null && asyncResult.isSucceeded()) {
                successHandler.handle(asyncResult.getResult());
            }

            if (failureHandler != null && asyncResult != null && asyncResult.isFailed()) {
                failureHandler.handle(asyncResult.getCause());
            }
        } finally {
            countDown();
        }
    }

    private void countDown() {
        if (!latchObjectRef.compareAndSet(null, new LatchObject(null))) {
            CountDownLatch latch = latchObjectRef.get().latch;
            // 被调用多次时可能为null
            if (latch != null)
                latch.countDown();
        }
    }
}
