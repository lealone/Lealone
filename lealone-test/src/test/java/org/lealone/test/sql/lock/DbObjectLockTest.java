/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.lock;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DbObjectLockTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        run(true);
        run(false);
    }

    private void run(final boolean isAutoCommit) throws Exception {
        Thread t1 = new Thread(() -> {
            testDbObjectLock(isAutoCommit);
        });
        Thread t2 = new Thread(() -> {
            testDbObjectLock(isAutoCommit);
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private void testDbObjectLock(boolean isAutoCommit) {
        try {
            Connection conn = getConnection();
            if (!isAutoCommit)
                conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("set LOCK_TIMEOUT 200000");
            stmt.executeUpdate("create table IF NOT EXISTS DbObjectLockTest(id int, name varchar(500))");
            if (!isAutoCommit)
                conn.commit();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
