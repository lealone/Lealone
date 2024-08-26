/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.TraceObject;

public class JdbcWrapper extends TraceObject implements Wrapper {

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public SQLException logAndConvert(Exception ex) { // 只是把protected变成public，允许在其他代码中调用
        return super.logAndConvert(ex);
    }

    public static void setAsyncResult(JdbcAsyncCallback<?> ac, Throwable cause) {
        // 转换成SQLException
        ac.setAsyncResult(DbException.toSQLException(cause));
    }
}
