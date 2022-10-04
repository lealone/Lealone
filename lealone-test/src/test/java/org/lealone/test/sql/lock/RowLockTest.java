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
import org.lealone.db.api.ErrorCode;
import org.lealone.test.sql.SqlTestBase;

public class RowLockTest extends SqlTestBase {
    @Test
    public void run() {
        createTable("RowLockTest");
        testUpdate();
        testDelete();
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
}
