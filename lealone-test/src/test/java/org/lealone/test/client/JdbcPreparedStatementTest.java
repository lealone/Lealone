/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcPreparedStatement;
import org.lealone.client.jdbc.JdbcResultSet;
import org.lealone.db.SysProperties;
import org.lealone.test.sql.SqlTestBase;

public class JdbcPreparedStatementTest extends SqlTestBase {

    private JdbcPreparedStatement ps;

    @Test
    public void run() throws Exception {
        testException();
        testMetaData();
        testFetchSize();
        testBatch();
        testAsync();
        testAsync2();
    }

    void testException() throws Exception {
        createTable();
        executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 2)");

        testSyncExecuteUpdateException();
        testAsyncExecuteUpdateException();

        testSyncExecuteQueryException();
        testAsyncExecuteQueryException();
    }

    private JdbcPreparedStatement prepareStatement(String sql) throws Exception {
        return prepareStatement(conn, sql);
    }

    private JdbcPreparedStatement prepareStatement(Connection conn, String sql) throws Exception {
        return (JdbcPreparedStatement) conn.prepareStatement(sql);
    }

    void testSyncExecuteUpdateException() throws Exception {
        try {
            ps = prepareStatement("INSERT INTO test(f1, f2) VALUES(1, 2)");
            // 主键重复，抛异常
            ps.executeUpdate();
            fail();
        } catch (SQLException e) {
        }

        Connection conn = getConnection();
        ps = prepareStatement(conn, "INSERT INTO test(f1, f2) VALUES(2, 3)");
        conn.close();
        try {
            // 连接已经关闭，抛异常
            ps.executeUpdate();
            fail();
        } catch (SQLException e) {
        }
    }

    void testAsyncExecuteUpdateException() throws Exception {
        // 主键重复，抛异常
        testAsyncExecuteUpdateException(conn, "INSERT INTO test(f1, f2) VALUES(1, 2)", false);

        Connection conn = getConnection();
        // 连接已经关闭，抛异常
        testAsyncExecuteUpdateException(conn, "INSERT INTO test(f1, f2) VALUES(2, 3)", true);
    }

    private void testAsyncExecuteUpdateException(Connection conn, String sql, boolean closeConnection)
            throws Exception {
        JdbcPreparedStatement ps = prepareStatement(conn, sql);
        if (closeConnection)
            conn.close();
        AtomicReference<Throwable> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        ps.executeUpdateAsync().onFailure(t -> {
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
            // 主键参数不合法，抛异常
            ps = prepareStatement("Select * From test Where f1=?");
            ps.setString(1, "abc");
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
        }

        Connection conn = getConnection();
        JdbcPreparedStatement ps = prepareStatement(conn, "Select * FROM test");
        conn.close();
        try {
            // 连接已经关闭，抛异常
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
        }
    }

    void testAsyncExecuteQueryException() throws Exception {
        // 主键参数不合法，抛异常
        testAsyncExecuteQueryException(conn, "Select * From test Where f1=?", false);

        Connection conn = getConnection();
        // 连接已经关闭，抛异常
        testAsyncExecuteQueryException(conn, "Select * FROM test", true);
    }

    private void testAsyncExecuteQueryException(Connection conn, String sql, boolean closeConnection) throws Exception {
        JdbcPreparedStatement ps = prepareStatement(conn, sql);
        if (closeConnection) {
            conn.close();
        } else {
            ps.setString(1, "abc");
        }
        AtomicReference<Throwable> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        ps.executeQueryAsync().onFailure(t -> {
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

    void testMetaData() throws Exception {
        createTable();
        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ResultSetMetaData md = ps.getMetaData();
        assertNull(md);
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertNotNull(pmd);
        assertEquals(2, pmd.getParameterCount());

        ps.setInt(1, 10);
        ps.setLong(2, 20);
        ps.executeUpdate();

        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            ps.setInt(1, i * 100);
            ps.setLong(2, 2 * i * 200);
            ps.executeUpdateAsync().onComplete(ar -> {
                latch.countDown();
            });
        }
        latch.await();

        ps = (JdbcPreparedStatement) conn.prepareStatement("SELECT * FROM test where f2 > ?");
        md = ps.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());

        CountDownLatch latch2 = new CountDownLatch(1);
        ps.setLong(1, 2);
        ps.executeQueryAsync().onComplete(ar -> {
            ResultSet rs = ar.getResult();
            try {
                while (rs.next()) {
                    System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            latch2.countDown();
        });
        latch2.await();
    }

    void testFetchSize() throws Exception {
        createTable();
        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        int count = 200;
        for (int i = 1; i <= count; i++) {
            ps.setInt(1, i * 10);
            ps.setLong(2, i * 20);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();

        sql = "SELECT * FROM test WHERE f1 >= ?";
        ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ps.setInt(1, 1);
        JdbcResultSet rs = (JdbcResultSet) ps.executeQuery();
        assertEquals(count, rs.getRowCount());
        assertEquals(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, rs.getCurrentRowCount());
        assertEquals(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, rs.getFetchSize());
        rs.close();
        ps.close();

        int fetchSize = 2;
        ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ps.setInt(1, 1);
        ps.setFetchSize(fetchSize); // 改变默认值
        rs = (JdbcResultSet) ps.executeQuery();
        assertEquals(count, rs.getRowCount());
        assertEquals(fetchSize, rs.getCurrentRowCount());
        assertEquals(fetchSize, rs.getFetchSize());
        rs.close();
        ps.close();
    }

    void testBatch() throws Exception {
        createTable();
        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ps.setInt(1, 1000);
        ps.setLong(2, 2000);
        ps.addBatch();
        ps.setInt(1, 8000);
        ps.setLong(2, 9000);
        ps.addBatch();
        int[] updateCounts = ps.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
    }

    void testAsync() throws Exception {
        createTable();

        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ps.setInt(1, 1);
        ps.setLong(2, 2);
        ps.executeUpdate();

        CountDownLatch latch = new CountDownLatch(1);
        ps.setInt(1, 2);
        ps.setLong(2, 2);
        ps.executeUpdateAsync().onComplete(res -> {
            System.out.println("updateCount: " + res.getResult());
            latch.countDown();
        });
        latch.await();
        ps.close();

        ps = (JdbcPreparedStatement) conn.prepareStatement("SELECT * FROM test where f2 = ?");
        ps.setLong(1, 2);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        }
        rs.close();
        CountDownLatch latch2 = new CountDownLatch(1);
        JdbcPreparedStatement ps2 = ps;
        ps2.setLong(1, 2);
        ps2.executeQueryAsync().onComplete(res -> {
            ResultSet rs2 = res.getResult();
            try {
                while (rs2.next()) {
                    System.out.println("f1=" + rs2.getInt(1) + " f2=" + rs2.getLong(2));
                }
                ps2.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch2.countDown();
            }
        });
        latch2.await();
    }

    // 测试连续的两个异步操作会不会按正常的先后顺序被后端执行
    void testAsync2() throws Exception {
        createTable();

        CountDownLatch latch = new CountDownLatch(2);
        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ps.setInt(1, 10);
        ps.setLong(2, 20);
        ps.executeUpdateAsync().onComplete(res -> {
            latch.countDown();
        });
        ps.setInt(1, 100);
        ps.setLong(2, 200);
        ps.executeUpdateAsync().onComplete(res -> {
            latch.countDown();
        });
        latch.await();
        ps.close();

        AtomicInteger count = new AtomicInteger();
        CountDownLatch latch2 = new CountDownLatch(2);
        ps = (JdbcPreparedStatement) conn.prepareStatement("SELECT * FROM test WHERE f1 = ?");
        ps.setInt(1, 10);
        ps.executeQueryAsync().onComplete(ar -> {
            JdbcResultSet rs = (JdbcResultSet) ar.getResult();
            int rowCount = rs.getRowCount();
            count.addAndGet(rowCount);
            latch2.countDown();
        });
        ps.setInt(1, 100);
        ps.executeQueryAsync().onComplete(ar -> {
            JdbcResultSet rs = (JdbcResultSet) ar.getResult();
            int rowCount = rs.getRowCount();
            count.addAndGet(rowCount);
            latch2.countDown();
        });
        latch2.await();
        ps.close();
        assertEquals(2, count.get());
    }

    private void createTable() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS test");
        executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
    }
}
