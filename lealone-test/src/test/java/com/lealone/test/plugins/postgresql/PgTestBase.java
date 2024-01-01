/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;

import com.lealone.common.exceptions.DbException;
import com.lealone.test.sql.SqlTestBase;

public class PgTestBase extends SqlTestBase {

    public final static int TEST_PORT = 9510;

    @Before
    @Override
    public void setUpBefore() {
        try {
            conn = getPgConnection();
            stmt = conn.createStatement();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static Connection getPgConnection() throws Exception {
        String url = "jdbc:postgresql://localhost:" + TEST_PORT + "/postgres";
        Properties info = new Properties();
        info.put("user", "postgres");
        info.put("password", "postgres");
        return DriverManager.getConnection(url, info);
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
