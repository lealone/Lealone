/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client.jdbc;

import java.sql.SQLException;

import org.lealone.db.async.ConcurrentAsyncCallback;

public class JdbcAsyncCallback<T> extends ConcurrentAsyncCallback<T> {

    public T get(JdbcWrapper jw) throws SQLException {
        try {
            return get();
        } catch (Exception e) {
            throw jw.logAndConvert(e); // 抛出SQLException
        }
    }
}
