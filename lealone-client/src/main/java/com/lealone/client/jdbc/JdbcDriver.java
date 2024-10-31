/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;

/**
 * The database driver. An application should not use this class directly. 
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcDriver implements java.sql.Driver {

    private static JdbcDriver INSTANCE;

    static {
        load();
    }

    /**
     * Open a database connection.
     * This method should not be called by an application.
     * Instead, the method DriverManager.getConnection should be used.
     *
     * @param url the database URL
     * @param info the connection properties
     * @return the new connection or null if the URL is not supported
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            return getConnection(url, info).get();
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Check if the driver understands this URL.
     * This method should not be called by an application.
     *
     * @param url the database URL
     * @return if the driver understands the URL
     */
    @Override
    public boolean acceptsURL(String url) {
        if (url != null && url.startsWith(Constants.URL_PREFIX)) {
            return true;
        }
        return false;
    }

    /**
     * Get the major version number of the driver.
     * This method should not be called by an application.
     *
     * @return the major version number
     */
    @Override
    public int getMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    /**
     * Get the minor version number of the driver.
     * This method should not be called by an application.
     *
     * @return the minor version number
     */
    @Override
    public int getMinorVersion() {
        return Constants.VERSION_MINOR;
    }

    /**
     * Get the list of supported properties.
     * This method should not be called by an application.
     *
     * @param url the database URL
     * @param info the connection properties
     * @return a zero length array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    /**
     * Check if this driver is compliant to the JDBC specification.
     * This method should not be called by an application.
     *
     * @return true
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * [Not supported]
     */
    // ## Java 1.7 ##
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw DbException.getUnsupportedException("getParentLogger()");
    }

    /**
     * INTERNAL
     */
    public static synchronized JdbcDriver load() {
        if (INSTANCE == null) {
            INSTANCE = new JdbcDriver();
            try {
                DriverManager.registerDriver(INSTANCE);
            } catch (SQLException e) {
                DbException.traceThrowable(e);
            }
        }
        return INSTANCE;
    }

    /**
     * INTERNAL
     */
    public static synchronized void unload() {
        if (INSTANCE != null) {
            try {
                DriverManager.deregisterDriver(INSTANCE);
                INSTANCE = null;
            } catch (SQLException e) {
                DbException.traceThrowable(e);
            }
        }
    }

    public static Future<JdbcConnection> getConnection(String url) {
        return getConnection(url, null);
    }

    public static Future<JdbcConnection> getConnection(String url, String user, String password) {
        Properties info = new Properties();
        if (user != null) {
            info.put("user", user);
        }
        if (password != null) {
            info.put("password", password);
        }
        return getConnection(url, info);
    }

    public static Future<JdbcConnection> getConnection(String url, Properties info) {
        // 不需要调用acceptsURL检查url，构建ConnectionInfo对象时会检查
        if (info == null) {
            info = new Properties();
        }
        try {
            ConnectionInfo ci = new ConnectionInfo(url, info);
            return getConnection(ci);
        } catch (Throwable t) {
            return Future.failedFuture(DbException.toSQLException(t));
        }
    }

    public static Future<JdbcConnection> getConnection(ConnectionInfo ci) {
        AsyncCallback<JdbcConnection> ac = AsyncCallback.createConcurrentCallback();
        try {
            ci.getSessionFactory().createSession(ci).onComplete(ar -> {
                if (ar.isSucceeded()) {
                    JdbcConnection conn = new JdbcConnection(ar.getResult(), ci);
                    ac.setAsyncResult(conn);
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
            });
        } catch (Throwable t) { // getSessionFactory也可能抛异常
            ac.setAsyncResult(t);
        }
        return ac;
    }
}
