/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class SequenceLockedExceptionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("DROP SEQUENCE IF EXISTS seq_locked");
        executeUpdate("CREATE SEQUENCE IF NOT EXISTS seq_locked");
        executeUpdate("DROP TABLE IF EXISTS SequenceLockedExceptionTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS SequenceLockedExceptionTest (f1 int, f2 int)");

        CountDownLatch latch = new CountDownLatch(2);
        executeInNewThread(latch,
                "insert into SequenceLockedExceptionTest values(1, seq_locked.NEXTVAL)");
        executeInNewThread(latch,
                "insert into SequenceLockedExceptionTest values(2, seq_locked.NEXTVAL)");
        latch.await();
        sql = "SELECT count(*) FROM SequenceLockedExceptionTest where f1>=1";
        assertEquals(2, getIntValue(1, true));

        latch = new CountDownLatch(2);
        executeInNewThread(latch, "select seq_locked.NEXTVAL");
        executeInNewThread(latch, "select seq_locked.NEXTVAL");
        latch.await();
        sql = "SELECT seq_locked.NEXTVAL";
        assertEquals(5, getIntValue(1, true));

        latch = new CountDownLatch(2);
        executeInNewThread(latch,
                "update SequenceLockedExceptionTest set f2=seq_locked.NEXTVAL where f1=1");
        executeInNewThread(latch,
                "update SequenceLockedExceptionTest set f2=seq_locked.NEXTVAL where f1=2");
        latch.await();
        sql = "SELECT seq_locked.CURRVAL";
        assertEquals(7, getIntValue(1, true));
        sql = "SELECT count(*) FROM SequenceLockedExceptionTest where f2>=5";
        assertEquals(2, getIntValue(1, true));
    }

    private void executeInNewThread(CountDownLatch latch, String sql) {
        new Thread(() -> {
            try {
                Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("set lock_timeout 1000000");
                conn.setAutoCommit(false);
                stmt.execute(sql);
                conn.commit();
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
