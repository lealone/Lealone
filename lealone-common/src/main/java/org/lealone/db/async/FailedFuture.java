/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import org.lealone.common.exceptions.DbException;

class FailedFuture<T> implements Future<T> {

    private final Throwable cause;

    public FailedFuture(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public T get() {
        throw DbException.convert(cause);
    }

    @Override
    public T get(long timeoutMillis) {
        return get();
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        handler.handle(cause);
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        handler.handle(new AsyncResult<>(cause));
        return this;
    }
}
