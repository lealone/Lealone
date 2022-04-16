/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.Constants;
import org.lealone.db.LealoneDatabase;

public class JdbcConnectionTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        testTransactionIsolationLevel();
        testNetworkTimeout();
        testSetAndGetSchema();

        int count = 1;
        count = 1;
        for (int i = 0; i < count; i++) {
            long t1 = System.currentTimeMillis();
            run2();
            long t2 = System.currentTimeMillis();
            System.out.println("loop: " + (i + 1) + ", time: " + (t2 - t1) + " ms");
        }
    }

    void testTransactionIsolationLevel() throws Exception {
        int til = conn.getTransactionIsolation();
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, til); // 默认是读已提交级别
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        conn.setTransactionIsolation(til);

        try {
            conn.setTransactionIsolation(-1);
            fail();
        } catch (SQLException e) {
        }
    }

    void testNetworkTimeout() throws Exception {
        int milliseconds = conn.getNetworkTimeout();
        assertEquals(NETWORK_TIMEOUT_MILLISECONDS, milliseconds);
        conn.setNetworkTimeout(null, 3000);
        assertEquals(3000, conn.getNetworkTimeout());
        conn.setNetworkTimeout(null, milliseconds);
        try {
            conn.setNetworkTimeout(null, -1);
            fail();
        } catch (SQLException e) {
        }
    }

    void testSetAndGetSchema() throws Exception {
        executeUpdate("DROP SCHEMA IF EXISTS testSetAndGetSchema");
        executeUpdate("CREATE SCHEMA IF NOT EXISTS testSetAndGetSchema AUTHORIZATION root");
        String schemaName = conn.getSchema().toUpperCase();
        assertEquals(Constants.SCHEMA_MAIN, schemaName.toUpperCase());
        conn.setSchema("testSetAndGetSchema");
        assertEquals("testSetAndGetSchema".toUpperCase(), conn.getSchema().toUpperCase());
        conn.setSchema(Constants.SCHEMA_MAIN);
    }

    public void run2() throws Exception {
        // addConnectionParameter(ConnectionSetting.TRACE_ENABLED.name(), true);
        // addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", TraceSystem.DEBUG);

        Connection conn0 = getConnection(LealoneDatabase.NAME);
        JdbcStatement stmt0 = (JdbcStatement) conn0.createStatement();
        stmt0.execute("CREATE TABLE IF NOT EXISTS JdbcConnectionTest (f1 int, f2 long)");

        stmt0.executeUpdate("DROP TABLE IF EXISTS test");
        stmt0.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        stmt0.executeUpdate("INSERT INTO test(f1, f2) VALUES(3, 2)");
        stmt0.execute("SELECT * FROM test");
        ResultSet rs = stmt0.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        conn0.close();

        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                Connection conn;
                try {
                    conn = getConnection(LealoneDatabase.NAME);
                    JdbcStatement stmt = (JdbcStatement) conn.createStatement();
                    // stmt.executeQuery("SELECT * FROM SYS");
                    String sql = "show tables";

                    sql = "insert into JdbcConnectionTest(f1,f2) values(1,2)";
                    int asyncCount = 20;
                    CountDownLatch asyncLatch = new CountDownLatch(asyncCount);
                    for (int j = 0; j < asyncCount; j++) {

                        stmt.executeUpdateAsync(sql).onComplete(ar -> {
                            asyncLatch.countDown();
                        });

                        // stmt.executeQueryAsync(sql, ar -> {
                        // try {
                        // if (ar.isSucceeded()) {
                        // ResultSet rs = ar.getResult();
                        // try {
                        // while (rs.next()) {
                        // System.out.println(rs.getString(1));
                        // }
                        // } catch (SQLException e) {
                        // e.printStackTrace();
                        // }
                        // }
                        // } finally {
                        // asyncLatch.countDown();
                        // }
                        // });
                    }
                    asyncLatch.await();
                    // stmt.execute("CREATE TABLE IF NOT EXISTS JdbcConnectionTest (f1 int, f2 long)");

                    // ResultSet rs = stmt.executeQuery("SELECT * FROM SYS");
                    // assertTrue(rs.next());
                    // assertEquals(1, rs.getInt(1));
                    // assertEquals(2, rs.getLong(2));

                    // stmt.executeUpdate("DELETE FROM JdbcConnectionTest WHERE f1 = 1");

                    stmt.close();
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
    }
}
