/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.db.ConnectionSetting;
import org.lealone.test.sql.SqlTestBase;

public class ReaCommittedTest extends SqlTestBase {

    public ReaCommittedTest() {
        addConnectionParameter(ConnectionSetting.IS_SHARED, "false");
    }

    @Test
    public void run() throws Exception {
        testInsert();
        testUpdate();
        testDelete();
    }

    void init(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS rctest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS rctest (f1 int primary key, f2 long)");
    }

    void testInsert() throws Exception {
        init(stmt);
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);

        Thread t1 = new Thread(() -> {
            try {
                Statement stmt1 = conn1.createStatement();
                stmt1.executeUpdate("INSERT INTO rctest(f1, f2) VALUES(1, 1)");
                stmt1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                Connection conn2 = getConnection();
                conn2.setAutoCommit(false);
                conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                Statement stmt2 = conn2.createStatement();
                ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rctest WHERE f2 = 1");
                assertFalse(rs2.next());
                rs2.close();

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

        conn1.commit();
        conn1.close();

        Connection conn3 = getConnection();
        conn3.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement stmt3 = conn3.createStatement();
        ResultSet rs3 = stmt3.executeQuery("SELECT f2 FROM rctest WHERE f2 = 1");
        assertTrue(rs3.next());
        rs3.close();
        stmt3.close();
        conn3.close();
    }

    void testUpdate() throws Exception {
        Connection conn1 = getConnection();
        Statement stmt1 = conn1.createStatement();
        init(stmt1);
        stmt1.executeUpdate("INSERT INTO rctest(f1, f2) VALUES(1, 1)");

        conn1.setAutoCommit(false);
        stmt1.executeUpdate("UPDATE rctest SET f2 = 2 WHERE f1 = 1");

        Connection conn2 = getConnection();
        conn2.setAutoCommit(false);
        conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement stmt2 = conn2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rctest WHERE f1 = 1");
        assertTrue(rs2.next());
        assertEquals(1, rs2.getLong(1));
        rs2.close();

        conn1.commit();

        rs2 = stmt2.executeQuery("SELECT f2 FROM rctest WHERE f1 = 1");
        assertTrue(rs2.next());
        assertEquals(2, rs2.getLong(1));
        rs2.close();

        conn1.close();
        conn2.close();
    }

    void testDelete() throws Exception {
        Connection conn1 = getConnection();
        Statement stmt1 = conn1.createStatement();
        init(stmt1);
        stmt1.executeUpdate("INSERT INTO rctest(f1, f2) VALUES(1, 1)");

        conn1.setAutoCommit(false);
        stmt1.executeUpdate("DELETE FROM rctest WHERE f1 = 1");

        Connection conn2 = getConnection();
        conn2.setAutoCommit(false);
        conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement stmt2 = conn2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rctest WHERE f1 = 1");
        assertTrue(rs2.next());
        rs2.close();

        conn1.commit();

        rs2 = stmt2.executeQuery("SELECT f2 FROM rctest WHERE f1 = 1");
        assertFalse(rs2.next());
        rs2.close();

        conn1.close();
        conn2.close();
    }
}
