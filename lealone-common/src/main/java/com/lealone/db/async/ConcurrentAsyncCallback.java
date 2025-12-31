/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;

public class ConcurrentAsyncCallback<T> extends AsyncCallback<T> {

    private volatile AsyncResultHandler<T> completeHandler;
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

    @Override
    protected T await(long timeoutMillis) {
        if (asyncResult != null)
            return getResult(asyncResult);
        Thread t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            Scheduler scheduler = ((SchedulerThread) t).getScheduler();
            scheduler.await(this, timeoutMillis);
            return getResult(asyncResult);
        }
        if (latchObjectRef.compareAndSet(null, new LatchObject(new CountDownLatch(1)))) {
            CountDownLatch latch = latchObjectRef.get().latch;
            try {
                if (timeoutMillis > 0)
                    latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
                else
                    latch.await();
                if (asyncResult != null && asyncResult.isFailed())
                    throw DbException.convert(asyncResult.getCause());
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            }
            if (asyncResult != null)
                return asyncResult.getResult();
            else
                return null;
        } else {
            return getResult(asyncResult);
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
    public synchronized Future<T> onComplete(AsyncResultHandler<T> handler) {
        completeHandler = handler;
        if (asyncResult != null) {
            handler.handle(asyncResult);
        }
        return this;
    }

    @Override
    public void setDbException(DbException e, boolean cancel) {
        setAsyncResult(e);
        if (cancel)
            countDown();
    }

    @Override
    public AsyncResult<T> getAsyncResult() {
        return asyncResult;
    }

    @Override
    public synchronized void setAsyncResult(AsyncResult<T> asyncResult) { // 复制集群场景可能会调用多次
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
