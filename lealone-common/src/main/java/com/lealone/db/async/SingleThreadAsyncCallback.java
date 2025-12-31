/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;

// 回调函数都在单线程中执行，也就是在当前调度线程中执行，可以优化回调的整个过程
public class SingleThreadAsyncCallback<T> extends AsyncCallback<T> {

    private AsyncResultHandler<T> completeHandler;
    private AsyncHandler<T> successHandler;
    private AsyncHandler<Throwable> failureHandler;
    private AsyncResult<T> asyncResult;

    @Override
    protected T await(long timeoutMillis) {
        // 使用阻塞IO时已经有结果就不需要等了
        if (asyncResult != null)
            return getResult(asyncResult);
        Thread t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            Scheduler scheduler = ((SchedulerThread) t).getScheduler();
            scheduler.await(this, timeoutMillis);
            return getResult(asyncResult);
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
    public Future<T> onComplete(AsyncResultHandler<T> handler) {
        completeHandler = handler;
        if (asyncResult != null) {
            handler.handle(asyncResult);
        }
        return this;
    }

    @Override
    public void setDbException(DbException e, boolean cancel) {
        setAsyncResult(e);
    }

    @Override
    public AsyncResult<T> getAsyncResult() {
        return asyncResult;
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
