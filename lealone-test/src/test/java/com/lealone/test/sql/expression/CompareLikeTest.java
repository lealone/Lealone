/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.expression;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class CompareLikeTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS CompareLikeTest");
        stmt.executeUpdate("create table IF NOT EXISTS CompareLikeTest(id int, name varchar(500))");

        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into CompareLikeTest(id, name) values(3, 'b3')");

        // ESCAPE后只能接一个字符
        // JdbcSQLException: Error in LIKE ESCAPE: "%kk";
        sql = "SELECT id,name FROM CompareLikeTest where name like '%2%' ESCAPE '%kk'";

        sql = "SELECT id,name FROM CompareLikeTest where name like '%2%' ESCAPE '%'";
        // sql = "SELECT id,name FROM CompareLikeTest where name like '%2%' ESCAPE 'a'";
        sql = "SELECT id,name FROM CompareLikeTest where name like '%2%'";
        assertEquals(2, printResultSet());
    }
}
