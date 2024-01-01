/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;

import com.lealone.common.exceptions.DbException;
import com.lealone.test.sql.SqlTestBase;

public class MySQLTestBase extends SqlTestBase {

    public final static int TEST_PORT = 9410;

    @Before
    @Override
    public void setUpBefore() {
        try {
            conn = getMySQLConnection();
            stmt = conn.createStatement();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static Connection getMySQLConnection() throws Exception {
        return getMySQLConnection(true, TEST_PORT);
    }

    public static Connection getMySQLConnection(String db) throws Exception {
        return getMySQLConnection(db, true, TEST_PORT);
    }

    public static Connection getMySQLConnection(boolean autoCommit, int port) throws Exception {
        String db = "mysql";
        return getMySQLConnection(db, autoCommit, port);
    }

    public static Connection getMySQLConnection(String db, boolean autoCommit, int port)
            throws Exception {
        // String driver = "com.mysql.jdbc.Driver";
        // Class.forName(driver);
        String password = "";
        // db = "test";
        // password = "zhh";

        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        // info.put("useServerPrepStmts", "true");
        // info.put("cachePrepStmts", "true");
        info.put("rewriteBatchedStatements", "true");
        info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

        Connection conn = DriverManager.getConnection(url, info);
        conn.getTransactionIsolation();
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    public static void sqlException(SQLException e) {
        while (e != null) {
            System.err.println("SQLException:" + e);
            System.err.println("-----------------------------------");
            System.err.println("Message  : " + e.getMessage());
            System.err.println("SQLState : " + e.getSQLState());
            System.err.println("ErrorCode: " + e.getErrorCode());
            System.err.println();
            System.err.println();
            e = e.getNextException();
        }
    }
}
