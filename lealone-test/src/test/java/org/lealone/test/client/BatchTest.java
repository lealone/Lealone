/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.common.exceptions.DbException;

public class BatchTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        init();
        testStatementBatch();
        testPreparedStatementBatch();
        // testConcurrentBatch();
    }

    void init() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS BatchTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS BatchTest(f1 int, f2 int)");
    }

    void testStatementBatch() throws Exception {
        testStatementBatch(stmt, 5);
    }

    void testStatementBatch(Statement stmt, int count) throws Exception {
        stmt.clearBatch();
        for (int i = 1; i <= count; i++) {
            stmt.addBatch("INSERT INTO BatchTest(f1, f2) VALUES(" + i + "," + (i * 2) + ")");
        }

        int[] result = stmt.executeBatch();
        assertEquals(count, result.length);
        for (int i = 1; i <= count; i++) {
            assertEquals(1, result[i - 1]);
        }

        stmt.clearBatch();
        result = stmt.executeBatch();
        assertEquals(0, result.length);
    }

    void testPreparedStatementBatch() throws Exception {
        sql = "INSERT INTO BatchTest(f1, f2) VALUES(?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= 5; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 2);
            ps.addBatch();
        }

        int[] result = ps.executeBatch();
        assertEquals(5, result.length);
        for (int i = 1; i <= 5; i++) {
            assertEquals(1, result[i - 1]);
        }

        ps.clearBatch();
        result = ps.executeBatch();
        assertEquals(0, result.length);

        ps.close();
    }

    Connection getConn() {
        try {
            return getConnection(dbName);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    void testConcurrentBatch() throws Exception {
        int size = 10;
        CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            new Thread(() -> {
                Connection conn = getConn();
                try {
                    Statement stmt = conn.createStatement();
                    for (int j = 0; j < 100; j++) {
                        testStatementBatch(stmt, 150);
                    }
                    stmt.close();
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                latch.countDown();
            }).start();
        }
        latch.await();
    }
}
