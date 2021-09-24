/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ConditionExistsTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS ConditionExistsTest");
        stmt.executeUpdate("create table IF NOT EXISTS ConditionExistsTest(id int, name varchar(500))");

        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into ConditionExistsTest(id, name) values(3, 'b3')");

        sql = "select * from ConditionExistsTest where name>'b' and EXISTS(select name from ConditionExistsTest where id=1)";
        sql = "select * from ConditionExistsTest where name>'b' or EXISTS(select name from ConditionExistsTest where id=1)";
        sql = "select * from ConditionExistsTest where name>'c' or "
                + "EXISTS(select count(*) from ConditionExistsTest where id=4)";

        assertEquals(6, printResultSet());
    }
}
