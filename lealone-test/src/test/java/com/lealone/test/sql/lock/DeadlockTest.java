/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.lock;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;
import com.lealone.test.sql.SqlTestBase;

public class DeadlockTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        // stmt.executeUpdate("set DEFAULT_LOCK_TIMEOUT 2000");
        stmt.executeUpdate("drop table IF EXISTS DeadlockTest1");
        stmt.executeUpdate("drop table IF EXISTS DeadlockTest2");
        stmt.executeUpdate(
                "create table IF NOT EXISTS DeadlockTest1(id int, name varchar(500), b boolean)");
        stmt.executeUpdate(
                "create table IF NOT EXISTS DeadlockTest2(id int, name varchar(500), b boolean)");

        stmt.executeUpdate("insert into DeadlockTest1(id, name, b) values(1, 'a1', true)");
        stmt.executeUpdate("insert into DeadlockTest2(id, name, b) values(1, 'a1', true)");

        Thread t1 = new Thread(() -> {
            try {
                Connection conn = DeadlockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("set LOCK_TIMEOUT 500");
                stmt.executeUpdate("update DeadlockTest1 set name = 'a2' where id = 1");
                stmt.executeUpdate("update DeadlockTest2 set name = 'a2' where id = 1");
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                assertErrorCode(e, ErrorCode.DEADLOCK_1);
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Connection conn = DeadlockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("set LOCK_TIMEOUT 500");
                stmt.executeUpdate("update DeadlockTest2 set name = 'a2' where id = 1");
                stmt.executeUpdate("update DeadlockTest1 set name = 'a2' where id = 1");
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                assertErrorCode(e, ErrorCode.DEADLOCK_1);
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}
