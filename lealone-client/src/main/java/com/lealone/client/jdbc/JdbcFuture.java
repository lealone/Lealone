/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.jdbc;

import java.sql.SQLException;

import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.async.Future;

public class JdbcFuture<T> {

    private final Future<T> future;
    private final JdbcWrapper jw;

    public JdbcFuture(Future<T> future, JdbcWrapper jw) {
        this.future = future;
        this.jw = jw;
    }

    public Future<T> getFuture() {
        return future;
    }

    public T get() throws SQLException {
        try {
            return future.get();
        } catch (Exception e) {
            throw jw.logAndConvert(e);
        }
    }

    public Future<T> onComplete(AsyncResultHandler<T> handler) {
        return future.onComplete(handler);
    }
}
