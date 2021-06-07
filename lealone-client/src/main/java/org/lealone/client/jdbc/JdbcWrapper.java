/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceObject;

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
}
