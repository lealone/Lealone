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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcResultSet;
import org.lealone.client.jdbc.JdbcStatement;

public class JdbcStatementTest extends ClientTestBase {

    public JdbcStatementTest() {
        // enableTrace(TraceSystem.DEBUG);
    }

    @Test
    public void run() throws Exception {
        testException();
        testExecute();
        testExecuteQuery();
        testExecuteUpdate();
        testBatch();
        testAsync();
        testAsync2();
        testCancel();
        testQueryTimeout();
        testGeneratedKeys();
    }

    void testException() throws Exception {
        testSyncExecuteUpdateException();
        testAsyncExecuteUpdateException();

        testSyncExecuteQueryException();
        testAsyncExecuteQueryException();
    }

    void testSyncExecuteUpdateException() throws Exception {
        try {
            // 语法错误，抛异常
            stmt.executeUpdate("CREATE TABLE4444 test (f1 int)");
            fail();
        } catch (SQLException e) {
        }

        Connection conn = getConnection();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        conn.close();
        try {
            // 连接已经关闭，抛异常
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int, f2 long)");
            fail();
        } catch (SQLException e) {
        }
    }

    void testAsyncExecuteUpdateException() throws Exception {
        // 语法错误，，抛异常
        testAsyncExecuteUpdateException((JdbcStatement) stmt, "CREATE TABLE4444 test (f1 int)");

        Connection conn = getConnection();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        conn.close();
        // 连接已经关闭，抛异常
        testAsyncExecuteUpdateException(stmt, "CREATE TABLE IF NOT EXISTS test (f1 int, f2 long)");
    }

    private void testAsyncExecuteUpdateException(JdbcStatement stmt, String sql) {
        AtomicReference<Throwable> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        stmt.executeUpdateAsync(sql).onFailure(t -> {
            ref.set(t);
            latch.countDown();
        });
        try {
            latch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(ref.get() instanceof SQLException);
    }

    void testSyncExecuteQueryException() throws Exception {
        try {
            // 语法错误，，抛异常
            stmt.executeQuery("Select * FROM4444 test");
            fail();
        } catch (SQLException e) {
        }

        Connection conn = getConnection();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        conn.close();
        try {
            // 连接已经关闭，抛异常
            stmt.executeQuery("Select * FROM test");
            fail();
        } catch (SQLException e) {
        }
    }

    void testAsyncExecuteQueryException() throws Exception {
        // 语法错误，抛异常
        testAsyncExecuteQueryException((JdbcStatement) stmt, "Select * FROM4444 test");

        Connection conn = getConnection();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        conn.close();
        // 连接已经关闭，抛异常
        testAsyncExecuteQueryException(stmt, "Select * FROM test");
    }

    private void testAsyncExecuteQueryException(JdbcStatement stmt, String sql) {
        AtomicReference<Throwable> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        stmt.executeQueryAsync(sql).onFailure(t -> {
            ref.set(t);
            latch.countDown();
        });
        try {
            latch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(ref.get() instanceof SQLException);
    }

    void testExecute() throws Exception {
        stmt.execute("/* test */DROP TABLE IF EXISTS test");
        stmt.execute("CREATE TABLE IF NOT EXISTS test (f1 int, f2 long)");
        boolean ret = stmt.execute("INSERT INTO test(f1, f2) VALUES(1, 2)");
        assertFalse(ret);
        assertEquals(1, stmt.getUpdateCount());
        ret = stmt.execute("SELECT f1, f2 FROM test");
        assertTrue(ret);
        assertNotNull(stmt.getResultSet());
    }

    void testExecuteQuery() throws Exception {
        createTable();
        executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 2)");
        ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));
    }

    void testExecuteUpdate() throws Exception {
        createTable();
        assertEquals(1, executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 2)"));
    }

    void testCancel() throws Exception {
        createTable();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        CountDownLatch latch = new CountDownLatch(1);
        stmt.executeQueryAsync("SELECT f1, f2 FROM test").onComplete(ar -> {
            latch.countDown();
        });
        stmt.cancel();
        latch.await();
    }

    void testBatch() throws SQLException {
        stmt.addBatch("INSERT INTO test(f1, f2) VALUES(1000, 2000)");
        stmt.addBatch("INSERT INTO test(f1, f2) VALUES(8000, 9000)");
        int[] updateCounts = stmt.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
    }

    void testAsync() throws Exception {
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();

        stmt.executeUpdateAsync("DROP TABLE IF EXISTS test").onSuccess(updateCount -> {
            System.out.println("updateCount: " + updateCount);
        }).onFailure(e -> {
            e.printStackTrace();
        }).get();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        String sql = "INSERT INTO test(f1, f2) VALUES(1, 2)";
        stmt.executeUpdate(sql);

        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(3, 3)").onSuccess(updateCount -> {
            System.out.println("updateCount: " + updateCount);
        }).onFailure(e -> {
            e.printStackTrace();
        }).get();

        CountDownLatch latch = new CountDownLatch(1);
        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(2, 2)").onComplete(res -> {
            if (res.isSucceeded()) {
                System.out.println("updateCount: " + res.getResult());
            } else {
                res.getCause().printStackTrace();
                return;
            }

            try {
                stmt.executeQueryAsync("SELECT * FROM test where f2 = 2").onComplete(res2 -> {
                    if (res2.isSucceeded()) {
                        ResultSet rs = res2.getResult();
                        try {
                            while (rs.next()) {
                                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        res2.getCause().printStackTrace();
                    }
                    latch.countDown();
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        latch.await();
    }

    // 测试连续的两个异步操作会不会按正常的先后顺序被后端执行
    void testAsync2() throws Exception {
        createTable();

        CountDownLatch latch = new CountDownLatch(2);
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(10, 20)").onComplete(res -> {
            latch.countDown();
        });
        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(100, 200)").onComplete(res -> {
            latch.countDown();
        });
        latch.await();
        stmt.close();

        AtomicInteger count = new AtomicInteger();
        CountDownLatch latch2 = new CountDownLatch(2);
        stmt = (JdbcStatement) conn.createStatement();
        stmt.executeQueryAsync("SELECT * FROM test WHERE f1 = 10").onComplete(ar -> {
            JdbcResultSet rs = (JdbcResultSet) ar.getResult();
            int rowCount = rs.getRowCount();
            count.addAndGet(rowCount);
            latch2.countDown();
        });
        stmt.executeQueryAsync("SELECT * FROM test WHERE f1 = 100").onComplete(ar -> {
            JdbcResultSet rs = (JdbcResultSet) ar.getResult();
            int rowCount = rs.getRowCount();
            count.addAndGet(rowCount);
            latch2.countDown();
        });
        latch2.await();
        stmt.close();
        assertEquals(2, count.get());
    }

    private void createTable() {
        executeUpdate("DROP TABLE IF EXISTS test");
        executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int, f2 long)");
    }

    void testQueryTimeout() throws Exception {
        try {
            stmt.setQueryTimeout(-1);
            fail();
        } catch (SQLException e) {
        }
        int seconds = stmt.getQueryTimeout();
        seconds = 2;
        stmt.setQueryTimeout(seconds);
        assertEquals(seconds, stmt.getQueryTimeout());
    }

    void testGeneratedKeys() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS testGeneratedKeys");
        executeUpdate("CREATE TABLE IF NOT EXISTS testGeneratedKeys (f1 int auto_increment, f2 long)");
        executeUpdate("INSERT INTO testGeneratedKeys(f2) VALUES(9000)");

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();

        executeUpdate("INSERT INTO testGeneratedKeys(f2) VALUES(9001)");
        executeUpdate("INSERT INTO testGeneratedKeys(f2) VALUES(9002)");
        rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs.close();
    }
}
