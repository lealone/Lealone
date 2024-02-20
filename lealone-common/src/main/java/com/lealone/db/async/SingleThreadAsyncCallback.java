/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.net.NetInputStream;

// 回调函数都在单线程中执行，也就是在当前调度线程中执行，可以优化回调的整个过程
public class SingleThreadAsyncCallback<T> extends AsyncCallback<T> {

    private AsyncHandler<AsyncResult<T>> completeHandler;
    private AsyncHandler<T> successHandler;
    private AsyncHandler<Throwable> failureHandler;
    private AsyncResult<T> asyncResult;

    public SingleThreadAsyncCallback() {
    }

    @Override
    public void setDbException(DbException e, boolean cancel) {
        setAsyncResult(e);
    }

    @Override
    public void run(NetInputStream in) {
        try {
            runInternal(in);
        } catch (Throwable t) {
            setAsyncResult(t);
        }
    }

    @Override
    protected T await(long timeoutMillis) {
        Scheduler scheduler = SchedulerThread.currentScheduler();
        if (scheduler != null) {
            scheduler.executeNextStatement();
            // 如果被锁住了，需要重试
            if (asyncResult == null) {
                while (asyncResult == null) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                    }
                    scheduler.executeNextStatement();
                }
            }

            if (asyncResult.isSucceeded())
                return asyncResult.getResult();
            else
                throw DbException.convert(asyncResult.getCause());
        } else {
            throw DbException.getInternalError();
        }
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        successHandler = handler;
        if (asyncResult != null && asyncResult.isSucceeded()) {
            handler.handle(asyncResult.getResult());
        }
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        failureHandler = handler;
        if (asyncResult != null && asyncResult.isFailed()) {
            handler.handle(asyncResult.getCause());
        }
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        completeHandler = handler;
        if (asyncResult != null) {
            handler.handle(asyncResult);
        }
        return this;
    }

    @Override
    public void setAsyncResult(AsyncResult<T> asyncResult) {
        this.asyncResult = asyncResult;
        if (completeHandler != null)
            completeHandler.handle(asyncResult);

        if (successHandler != null && asyncResult != null && asyncResult.isSucceeded())
            successHandler.handle(asyncResult.getResult());

        if (failureHandler != null && asyncResult != null && asyncResult.isFailed())
            failureHandler.handle(asyncResult.getCause());
    }
}
