/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.transaction;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class UpdateLossTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS UpdateLossTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS UpdateLossTest (f1 int primary key, f2 int)");
        stmt.executeUpdate("INSERT INTO UpdateLossTest(f1, f2) VALUES(1,10)");

        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);
        conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Statement stmt1 = conn1.createStatement();
        assertF2(10, stmt1);

        Connection conn2 = getConnection();
        Statement stmt2 = conn2.createStatement();
        stmt2.executeUpdate("update UpdateLossTest set f2=100 where f1=1");
        assertF2(100, stmt2);

        assertF2(10, stmt1); // 此时依然是10
        // 自动从TRANSACTION_REPEATABLE_READ切换到READ_COMMITTED
        stmt1.executeUpdate("update UpdateLossTest set f2=f2+1 where f1=1");
        assertF2(101, stmt1);
        conn1.commit();

        assertF2(101, stmt1);
        assertF2(101, stmt2);

        conn1.close();
        conn2.close();
    }

    private void assertF2(long expected, Statement stmt) throws Exception {
        rs = stmt.executeQuery("SELECT f2 FROM UpdateLossTest WHERE f1 = 1");
        rs.next();
        assertEquals(expected, getIntValue(1, true));
    }
}
