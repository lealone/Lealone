/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.expression;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class WildcardTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS WildcardTest");
        stmt.executeUpdate("create table IF NOT EXISTS WildcardTest(id int, name varchar(500))");

        stmt.executeUpdate("insert into WildcardTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into WildcardTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into WildcardTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into WildcardTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into WildcardTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into WildcardTest(id, name) values(3, 'b3')");

        sql = "select * from WildcardTest";
        sql = "select WildcardTest.* from WildcardTest";
        sql = "select public.WildcardTest.* from WildcardTest";
        assertEquals(6, printResultSet());
    }
}
