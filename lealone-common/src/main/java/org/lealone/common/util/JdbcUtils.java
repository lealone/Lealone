/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.sql.DataSource;

import org.lealone.common.message.DbException;

/**
 * This is a utility class with JDBC helper functions.
 */
public class JdbcUtils {

    private JdbcUtils() {
        // utility class
    }

    /**
     * Close a statement without throwing an exception.
     *
     * @param stat the statement or null
     */
    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a connection without throwing an exception.
     *
     * @param conn the connection or null
     */
    public static void closeSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a result set without throwing an exception.
     *
     * @param rs the result set or null
     */
    public static void closeSilently(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url, String user, String password) throws SQLException {
        Properties prop = new Properties();
        if (user != null) {
            prop.setProperty("user", user);
        }
        if (password != null) {
            prop.setProperty("password", password);
        }
        return getConnection(driver, url, prop);
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param prop the properties containing at least the user name and password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url, Properties prop) throws SQLException {
        Class<?> d = Utils.loadUserClass(driver);
        if (java.sql.Driver.class.isAssignableFrom(d)) {
            return DriverManager.getConnection(url, prop);
        } else if (javax.naming.Context.class.isAssignableFrom(d)) {
            // JNDI context
            try {
                Context context = (Context) d.newInstance();
                DataSource ds = (DataSource) context.lookup(url);
                String user = prop.getProperty("user");
                String password = prop.getProperty("password");
                if (StringUtils.isNullOrEmpty(user) && StringUtils.isNullOrEmpty(password)) {
                    return ds.getConnection();
                }
                return ds.getConnection(user, password);
            } catch (Exception e) {
                throw DbException.toSQLException(e);
            }
        } else {
            // don't know, but maybe it loaded a JDBC Driver
            return DriverManager.getConnection(url, prop);
        }
    }

}
