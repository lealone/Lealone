/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import java.sql.Savepoint;

import org.junit.Test;

import com.lealone.client.jdbc.JdbcConnection;
import com.lealone.test.sql.SqlTestBase;

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

        conn.setAutoCommit(false);
        executeUpdate("insert into TransactionStatementTest(id, name) values(2, 'a2')");
        Savepoint sp = conn.setSavepoint();
        executeUpdate("insert into TransactionStatementTest(id, name) values(3, 'a3')");
        conn.rollback(sp);
        conn.commit();
        sql = "SELECT count(*) FROM TransactionStatementTest where id=2";
        assertEquals(1, getIntValue(1, true));
        sql = "SELECT count(*) FROM TransactionStatementTest where id=3";
        assertEquals(0, getIntValue(1, true));
        conn.commit();

        executeUpdate("insert into TransactionStatementTest(id, name) values(3, 'a3')");
        ((JdbcConnection) conn).rollbackAsync().get();
        sql = "SELECT count(*) FROM TransactionStatementTest where id=3";
        assertEquals(0, getIntValue(1, true));
        conn.commit();

        executeUpdate("insert into TransactionStatementTest(id, name) values(3, 'a3')");
        sp = conn.setSavepoint("mySavepoint");
        executeUpdate("insert into TransactionStatementTest(id, name) values(4, 'a4')");
        conn.rollback(sp);
        ((JdbcConnection) conn).commitAsync().get();
        conn.setAutoCommit(true);

        sql = "SELECT count(*) FROM TransactionStatementTest where id=3";
        assertEquals(1, getIntValue(1, true));
        sql = "SELECT count(*) FROM TransactionStatementTest where id=4";
        assertEquals(0, getIntValue(1, true));
    }
}
