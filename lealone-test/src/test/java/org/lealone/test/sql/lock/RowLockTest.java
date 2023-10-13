/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.lock;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.common.util.JdbcUtils;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.api.ErrorCode;
import org.lealone.test.sql.SqlTestBase;

public class RowLockTest extends SqlTestBase {

    public RowLockTest() {
        addConnectionParameter(ConnectionSetting.IS_SHARED, "false");
    }

    @Test
    public void run() throws Exception {
        createTable("RowLockTest");
        testUpdate();
        testDelete();
        testMultiThreadsUpdate();
    }

    void testUpdate() {
        try {
            executeUpdate("INSERT INTO RowLockTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 51)");
            conn.setAutoCommit(false);
            sql = "UPDATE RowLockTest SET f1 = 'a2' WHERE pk = '02'";
            executeUpdate(sql);

            Connection conn2 = null;
            try {
                conn2 = getConnection();
                conn2.setAutoCommit(false);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300");
                stmt2.executeUpdate("UPDATE RowLockTest SET f1 = 'a3' WHERE pk = '02'");
                fail();
            } catch (Exception e) {
                assertLockTimeout(conn2, e);
            }

            conn.commit();
            conn.setAutoCommit(true);

            sql = "SELECT f1, f2, f3 FROM RowLockTest WHERE pk = '02'";
            assertEquals("a2", getStringValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void testDelete() {
        try {
            executeUpdate("INSERT INTO RowLockTest(pk, f1, f2, f3) VALUES('100', 'a1', 'b', 51)");
            conn.setAutoCommit(false);
            sql = "UPDATE RowLockTest SET f1 = 'a2' WHERE pk = '100'";
            executeUpdate(sql);

            Connection conn2 = null;
            try {
                conn2 = getConnection();
                conn2.setAutoCommit(false);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300");
                stmt2.executeUpdate("DELETE FROM RowLockTest WHERE pk = '100'");
                fail();
            } catch (Exception e) {
                assertLockTimeout(conn2, e);
            }

            conn.commit();
            conn.setAutoCommit(true);

            sql = "SELECT f1, f2, f3 FROM RowLockTest WHERE pk = '100'";
            assertEquals("a2", getStringValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            conn.setAutoCommit(false);
            sql = "DELETE FROM RowLockTest WHERE pk = '100'";
            executeUpdate(sql);

            Connection conn2 = null;
            try {
                conn2 = getConnection();
                conn2.setAutoCommit(false);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300");
                stmt2.executeUpdate("UPDATE RowLockTest SET f1 = 'a2' WHERE pk = '100'");
                fail();
            } catch (Exception e) {
                assertLockTimeout(conn2, e);
            }

            conn.commit();
            conn.setAutoCommit(true);

            sql = "SELECT count(*) FROM RowLockTest WHERE pk = '100'";
            assertEquals(0, getIntValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void assertLockTimeout(Connection conn, Exception e) {
        assertErrorCode(e, ErrorCode.LOCK_TIMEOUT_1);
        if (conn != null)
            JdbcUtils.closeSilently(conn);
        System.out.println(e.getMessage());
    }

    void testMultiThreadsUpdate() throws Exception {
        executeUpdate("INSERT INTO RowLockTest(pk, f1, f2, f3) VALUES('200', 'a1', 'b', 51)");
        Thread t1 = new Thread(() -> {
            try {
                Connection conn1 = getConnection();
                Statement stmt1 = conn1.createStatement();
                stmt1.executeUpdate("SET LOCK_TIMEOUT = 300000");
                conn1.setAutoCommit(false);
                stmt1.executeUpdate("UPDATE RowLockTest SET f1 = 'a100' WHERE pk = '200'");
                stmt1.close();
                conn1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                Connection conn2 = getConnection();
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300000");
                conn2.setAutoCommit(false);
                stmt2.executeUpdate("UPDATE RowLockTest SET f1 = 'a200' WHERE pk = '200'");
                stmt2.close();
                conn2.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}
