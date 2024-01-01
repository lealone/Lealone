/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.transaction;

import org.junit.Test;

import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.test.sql.SqlTestBase;

public class CommitTest extends SqlTestBase {

    public CommitTest() {
    }

    @Test
    public void run() throws Exception {
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS CommitTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CommitTest (f1 int, f2 long)");
        conn.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO CommitTest(f1, f2) VALUES(1, 1)");
        stmt.executeUpdateAsync("commit");
        stmt.executeUpdateAsync("commit").get();
        sql = "SELECT count(*) FROM CommitTest WHERE f1 = 1";
        assertEquals(1, getIntValue(1, true));
    }
}
