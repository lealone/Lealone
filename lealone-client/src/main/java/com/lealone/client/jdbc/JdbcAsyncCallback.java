/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.jdbc;

import java.sql.SQLException;

import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.session.Session;

public class JdbcAsyncCallback<T> implements Future<T> {

    private AsyncCallback<T> ac;

    private JdbcAsyncCallback(boolean isSingleThread) {
        ac = AsyncCallback.create(isSingleThread);
    }

    public T get(JdbcWrapper jw) throws SQLException {
        try {
            return ac.get();
        } catch (Exception e) {
            throw jw.logAndConvert(e); // 抛出SQLException
        }
    }

    @Override
    public T get() {
        return ac.get();
    }

    @Override
    public T get(long timeoutMillis) {
        return ac.get(timeoutMillis);
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        return ac.onSuccess(handler);
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        return ac.onFailure(handler);
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        return ac.onComplete(handler);
    }

    public void setAsyncResult(Throwable cause) {
        ac.setAsyncResult(cause);
    }

    public void setAsyncResult(T result) {
        ac.setAsyncResult(result);
    }

    public void setAsyncResult(AsyncResult<T> asyncResult) {
        ac.setAsyncResult(asyncResult);
    }

    public static <T> JdbcAsyncCallback<T> create(Session session) {
        // JDBC使用阻塞IO时使用SingleThreadAsyncCallback
        return new JdbcAsyncCallback<>(session.isBio());
    }
}
