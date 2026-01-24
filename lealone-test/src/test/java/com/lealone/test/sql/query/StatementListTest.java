/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.query;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class StatementListTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS StatementListTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS StatementListTest(f1 int)");
        executeUpdate("INSERT INTO StatementListTest VALUES(10)");

        sql = "select * from StatementListTest where f1=10; show databases";
        assertEquals(1, printResultSet());
        sql = "select * from StatementListTest where f1=10; select * from StatementListTest where f1=10";
        assertEquals(1, printResultSet());
        sql = "select * from StatementListTest where f1=10; update StatementListTest set f1=20;";
        assertEquals(1, printResultSet());

        sql = "update StatementListTest set f1=20; show databases";
        assertEquals(1, executeUpdate());

        sql = "update StatementListTest set f1=20; select * from StatementListTest where f1=10";
        assertEquals(1, executeUpdate());
    }
}
