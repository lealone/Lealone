/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;

/**
 * The database driver. An application should not use this class directly. 
 */
public class JdbcDriver implements java.sql.Driver {

    private static final JdbcDriver INSTANCE = new JdbcDriver();

    private static volatile boolean registered;

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
            if (!acceptsURL(url)) {
                return null;
            }
            if (info == null) {
                info = new Properties();
            }
            return new JdbcConnection(url, info);
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
        if (!registered) {
            registered = true;

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
        if (registered) {
            registered = false;

            try {
                DriverManager.deregisterDriver(INSTANCE);
            } catch (SQLException e) {
                DbException.traceThrowable(e);
            }
        }
    }

    public static JdbcConnection getConnection(String url) throws SQLException {
        Properties info = new Properties();
        return getConnection(url, info);
    }

    public static JdbcConnection getConnection(String url, String user, String password) throws SQLException {
        Properties info = new Properties();
        if (user != null) {
            info.put("user", user);
        }
        if (password != null) {
            info.put("password", password);
        }
        return getConnection(url, info);
    }

    public static JdbcConnection getConnection(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        return new JdbcConnection(url, info);
    }

    public static void getConnectionAsync(String url, AsyncHandler<AsyncResult<JdbcConnection>> handler)
            throws SQLException {
        Properties info = new Properties();
        getConnectionAsync(url, info, handler);
    }

    public static void getConnectionAsync(String url, String user, String password,
            AsyncHandler<AsyncResult<JdbcConnection>> handler) throws SQLException {
        Properties info = new Properties();
        if (user != null) {
            info.put("user", user);
        }
        if (password != null) {
            info.put("password", password);
        }
        getConnectionAsync(url, info, handler);
    }

    public static void getConnectionAsync(String url, Properties info,
            AsyncHandler<AsyncResult<JdbcConnection>> handler) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        try {
            ConnectionInfo ci = new ConnectionInfo(url, info);
            ci.getSessionFactory().createSessionAsync(ci, ar -> {
                if (ar.isSucceeded()) {
                    Session s = ar.getResult();
                    JdbcConnection conn = new JdbcConnection(s, ci);
                    handler.handle(new AsyncResult<>(conn));
                } else {
                    handler.handle(new AsyncResult<>(ar.getCause()));
                }
            });
        } catch (Exception e) {
            handler.handle(new AsyncResult<>(e));
        }
    }
}
