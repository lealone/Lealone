/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;

//标识它的子类是进行单元测试的
public class UnitTestBase extends TestBase implements org.lealone.test.TestBase.SqlExecutor {

    public UnitTestBase() {
        initTransactionEngine();
    }

    @Override
    public void execute(String sql) {
        try (Connection conn = DriverManager.getConnection(getURL()); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int count(String sql) {
        try (Connection conn = DriverManager.getConnection(getURL()); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void explain(String sql) {
        try (Connection conn = DriverManager.getConnection(getURL()); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("EXPLAIN " + sql);
            if (rs.next()) {
                System.out.println();
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
    }

    @After
    public void tearDownAfter() {
        closeTransactionEngine();
    }

    public void runTest() {
        runTest(true, true);
    }

    public void runTest(boolean isEmbeddedMemoryMode, boolean closeTransactionEngine) {
        if (isEmbeddedMemoryMode) {
            setEmbedded(true);
            setInMemory(true);
        }
        System.setProperty("lealone.jdbc.url", getURL());
        try {
            test();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (closeTransactionEngine)
                closeTransactionEngine();
        }
    }

    protected void test() throws Exception {
        // do nothing
    }
}
