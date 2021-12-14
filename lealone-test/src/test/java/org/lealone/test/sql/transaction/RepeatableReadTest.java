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
import org.lealone.test.sql.SqlTestBase;

public class RepeatableReadTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        testInsert();
        testUpdate();
        testDelete();
    }

    void init(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS rrtest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS rrtest (f1 int primary key, f2 long)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS rrtest_idx2 ON rrtest(f2)");
    }

    void testInsert() throws Exception {
        Connection conn1 = getConnection();
        Statement stmt1 = conn1.createStatement();
        init(stmt1);

        conn1.setAutoCommit(false);
        stmt1.executeUpdate("INSERT INTO rrtest(f1, f2) VALUES(1, 1)");

        Connection conn2 = getConnection();
        conn2.setAutoCommit(false);
        conn2.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Statement stmt2 = conn2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertFalse(rs2.next());
        rs2.close();

        conn1.commit();

        rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertFalse(rs2.next());
        rs2.close();

        conn1.close();
        conn2.close();
    }

    void testUpdate() throws Exception {
        Connection conn1 = getConnection();
        Statement stmt1 = conn1.createStatement();
        init(stmt1);
        stmt1.executeUpdate("INSERT INTO rrtest(f1, f2) VALUES(1, 1)");

        conn1.setAutoCommit(false);
        stmt1.executeUpdate("UPDATE rrtest SET f2 = 2 WHERE f1 = 1");

        Connection conn2 = getConnection();
        conn2.setAutoCommit(false);
        conn2.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Statement stmt2 = conn2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertTrue(rs2.next());
        assertEquals(1, rs2.getLong(1));
        rs2.close();

        conn1.commit();

        rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertTrue(rs2.next());
        assertEquals(1, rs2.getLong(1));
        rs2.close();

        conn1.close();
        conn2.close();
    }

    void testDelete() throws Exception {
        Connection conn1 = getConnection();
        Statement stmt1 = conn1.createStatement();
        init(stmt1);
        stmt1.executeUpdate("INSERT INTO rrtest(f1, f2) VALUES(1, 1)");

        conn1.setAutoCommit(false);
        stmt1.executeUpdate("DELETE FROM rrtest WHERE f1 = 1");

        Connection conn2 = getConnection();
        conn2.setAutoCommit(false);
        conn2.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Statement stmt2 = conn2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertTrue(rs2.next());
        rs2.close();

        conn1.commit();

        rs2 = stmt2.executeQuery("SELECT f2 FROM rrtest WHERE f2 = 1");
        assertTrue(rs2.next());
        rs2.close();

        conn1.close();
        conn2.close();
    }
}
