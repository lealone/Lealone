/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;

import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.util.ThreadUtils;
import com.lealone.net.bio.BioNetFactory;
import com.lealone.net.nio.NioNetFactory;
import com.lealone.test.TestBase;

public class CRUDExample {

    public static void main(String[] args) throws Exception {
        crud();
        // perf();
    }

    public static void crud() throws Exception {
        Connection conn = null;

        conn = getBioConnection();
        crud(conn);

        conn = getNioConnection();
        crud(conn);

        conn = getEmbeddedConnection();
        crud(conn);
    }

    public static void perf() throws Exception {
        // startMultiThreads();
        Connection conn = null;
        int count = 100;
        for (int i = 0; i < count; i++) {
            // conn = getNioConnection();
            conn = getBioConnection();
            // conn = getEmbeddedConnection();

            long t1 = System.currentTimeMillis();
            crud(conn, false);
            long t2 = System.currentTimeMillis();
            System.out.println("crud time: " + (t2 - t1) + " ms");
        }
    }

    public static Connection getBioConnection() throws Exception {
        TestBase test = new TestBase();
        test.setNetFactoryName(BioNetFactory.NAME);
        return test.getConnection(LealoneDatabase.NAME);
    }

    public static Connection getNioConnection() throws Exception {
        TestBase test = new TestBase();
        test.setNetFactoryName(NioNetFactory.NAME);
        // test.addConnectionParameter(ConnectionSetting.SOCKET_RECV_BUFFER_SIZE, 4096 );
        test.addConnectionParameter(ConnectionSetting.MAX_PACKET_SIZE, 16 * 1024 * 1024);
        test.addConnectionParameter(ConnectionSetting.AUTO_RECONNECT, true);
        // test.addConnectionParameter(ConnectionSetting.SCHEDULER_COUNT, 1);
        return test.getConnection(LealoneDatabase.NAME);
    }

    public static Connection getEmbeddedConnection() throws Exception {
        TestBase test = new TestBase();
        test.setEmbedded(true);
        test.setInMemory(true);
        return test.getConnection(LealoneDatabase.NAME);
    }

    public static void crud(Connection conn) throws Exception {
        crud(conn, true);
    }

    public static void crud(Connection conn, boolean print) throws Exception {
        Statement stmt = conn.createStatement();
        crud(stmt, print);
        // asyncInsert(stmt);
        // batchInsert(stmt);
        // batchPreparedInsert(conn, stmt);
        // batchDelete(stmt);
        // testFetchSize(stmt);
        // getMetaData(conn, stmt);
        stmt.close();
        conn.close();
    }

    public static void createTable(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        String sql = "CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)";
        stmt.executeUpdate(sql);
    }

    public static void crud(Statement stmt, boolean print) throws Exception {
        createTable(stmt);
        stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
        stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertTrue(rs.next());
        if (print)
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
        rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertFalse(rs.next());
        rs.close();
    }

    public static void testFetchSize(Statement stmt) throws Exception {
        createTable(stmt);
        batchInsert(stmt);
        stmt.setFetchSize(10);
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        }
        rs.close();
    }

    public static void batchInsert(Statement stmt) throws Exception {
        for (int i = 1; i <= 60; i++)
            stmt.addBatch("INSERT INTO test(f1, f2) VALUES(" + i + ", " + i * 10 + ")");
        stmt.executeBatch();
    }

    public static void batchPreparedInsert(Connection conn, Statement stmt) throws Exception {
        createTable(stmt);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO test(f1, f2) VALUES(?,?)");
        for (int i = 1; i <= 60; i++) {
            ps.setInt(1, i);
            ps.setLong(2, i * 10);
            ps.addBatch();
        }
        ps.executeBatch();
        // ps.close();
    }

    public static void asyncInsert(Statement stmt0) throws Exception {
        JdbcStatement stmt = (JdbcStatement) stmt0;
        createTable(stmt);
        int size = 60;
        CountDownLatch latch = new CountDownLatch(size);
        for (int i = 1; i <= size; i++) {
            String sql = "INSERT INTO test(f1, f2) VALUES(" + i + ", " + i * 10 + ")";
            stmt.executeUpdateAsync(sql).onComplete(ar -> {
                if (ar.isFailed())
                    ar.getCause().printStackTrace();
                latch.countDown();
            });
        }
        latch.await();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test");
        rs.next();
        System.out.println("count=" + rs.getInt(1));
        Assert.assertEquals(size, rs.getInt(1));
        rs.close();
    }

    public static void batchDelete(Statement stmt) throws Exception {
        for (int i = 1; i <= 60; i++)
            stmt.executeUpdate("DELETE FROM test WHERE f1 =" + i);
    }

    public static void getMetaData(Connection conn, Statement stmt) throws Exception {
        createTable(stmt);
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM test");
        ResultSetMetaData md = ps.getMetaData();
        System.out.println("column count=" + md.getColumnCount());
        Assert.assertEquals(2, md.getColumnCount());
        ps.close();
    }

    public static void startMultiThreads() throws Exception {
        ThreadUtils.start("CRUDExample1", () -> {
            try {
                Connection conn = getNioConnection();
                crud(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        ThreadUtils.start("CRUDExample2", () -> {
            try {
                Connection conn = getNioConnection();
                crud(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        ThreadUtils.start("CRUDExample3", () -> {
            try {
                Connection conn = getNioConnection();
                crud(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
