/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

import com.lealone.common.exceptions.DbException;

public abstract class AsyncCallback<T> implements Future<T> {

    public AsyncCallback() {
    }

    protected abstract T await(long timeoutMillis);

    @Override
    public T get() {
        return await(-1);
    }

    @Override
    public T get(long timeoutMillis) {
        return await(timeoutMillis);
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncResultHandler<T> handler) {
        return this;
    }

    public static <T> AsyncCallback<T> createSingleThreadCallback() {
        return new SingleThreadAsyncCallback<>();
    }

    public static <T> AsyncCallback<T> createConcurrentCallback() {
        return new ConcurrentAsyncCallback<>();
    }

    public static <T> AsyncCallback<T> create(boolean isSingleThread) {
        return isSingleThread ? createSingleThreadCallback() : createConcurrentCallback();
    }

    public abstract void setDbException(DbException e, boolean cancel);

    public void setAsyncResult(Throwable cause) {
        setAsyncResult(new AsyncResult<>(cause));
    }

    public void setAsyncResult(T result) {
        setAsyncResult(new AsyncResult<>(result));
    }

    public abstract void setAsyncResult(AsyncResult<T> asyncResult);

    public abstract AsyncResult<T> getAsyncResult();

    public T getResult(AsyncResult<T> asyncResult) {
        if (asyncResult.isSucceeded())
            return asyncResult.getResult();
        else
            throw DbException.convert(asyncResult.getCause());
    }
}
