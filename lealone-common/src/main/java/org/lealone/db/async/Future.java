/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

public interface Future<T> {

    static <T> Future<T> succeededFuture(T result) {
        return new SucceededFuture<>(result);
    }

    static <T> Future<T> failedFuture(Throwable t) {
        return new FailedFuture<>(t);
    }

    T get();

    T get(long timeoutMillis);

    Future<T> onSuccess(AsyncHandler<T> handler);

    Future<T> onFailure(AsyncHandler<Throwable> handler);

    Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler);
}
