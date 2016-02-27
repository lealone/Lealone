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
import org.lealone.db.Session;

/**
 * The database driver. An application should not use this class directly. 
 */
public class JdbcDriver implements java.sql.Driver {

    private static final JdbcDriver INSTANCE = new JdbcDriver();
    private static final String DEFAULT_URL = Constants.CONN_URL_INTERNAL;

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
            if (url.equals(DEFAULT_URL)) {
                Session s = ConnectionInfo.getInternalSession();
                return new JdbcConnection(s, info.getProperty("user"), url);
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

}
