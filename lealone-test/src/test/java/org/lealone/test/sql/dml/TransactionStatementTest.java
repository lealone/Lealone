/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class TransactionStatementTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("drop table IF EXISTS TransactionStatementTest");
        executeUpdate("create table IF NOT EXISTS TransactionStatementTest(id int, name varchar(4))");

        executeUpdate("SET AUTOCOMMIT false");
        // executeUpdate("BEGIN");

        executeUpdate("insert into TransactionStatementTest(id, name) values(1, 'a1')");
        executeUpdate("insert into TransactionStatementTest(id, name) values(1, 'b1')");
        conn.rollback();

        sql = "SELECT count(*) FROM TransactionStatementTest";
        assertEquals(0, getIntValue(1, true));

        executeUpdate("SET AUTOCOMMIT true");
    }
}
