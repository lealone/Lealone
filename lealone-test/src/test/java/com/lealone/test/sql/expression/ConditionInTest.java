/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.expression;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class ConditionInTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS ConditionInTest");
        stmt.executeUpdate("create table IF NOT EXISTS ConditionInTest(id int, name varchar(500))");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS ConditionInTestIndex ON ConditionInTest(name)");

        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS ConditionInTestIndex2 ON ConditionInTest(id)");

        stmt.executeUpdate("insert into ConditionInTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into ConditionInTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into ConditionInTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into ConditionInTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into ConditionInTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into ConditionInTest(id, name) values(3, 'b3')");

        stmt.executeUpdate("drop table IF EXISTS SubqueryTest");
        stmt.executeUpdate("create table IF NOT EXISTS SubqueryTest(id int, name varchar(500))");

        stmt.executeUpdate("insert into SubqueryTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into SubqueryTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into SubqueryTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into SubqueryTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into SubqueryTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into SubqueryTest(id, name) values(3, 'b3')");

        sql = "select count(*) from ConditionInTest where id in()";
        sql = "select count(*) from ConditionInTest where not id in()";
        sql = "select count(*) from ConditionInTest where id in(3,4)";
        assertEquals(2, getIntValue(1, true));

        // 如id in(+(select id from SubqueryTest where id=1 and name='a1'))
        // Subquery的记录不能多于1条，所以在前面放+号就可以绕过isSelect()，此时返回的就是一个Subquery
        // 但是放减号是不行的，会得到一个Operation
        // +号会转成ConditionInSelect，而不再使用ConditionIn
        sql = "select count(*) from ConditionInTest where id in(+(select id from SubqueryTest where id=1 and name='a1'))";
        sql = "select count(*) from ConditionInTest where id in(-(select id from SubqueryTest where id=1 and name='a1'))";
        sql = "select count(*) from ConditionInTest where id in(3, (select id from SubqueryTest where id=1 and name='a1'))";
        printResultSet();

        sql = "select count(*) from ConditionInTest where null in(1,2)";
        // sql = "select count(*) from ConditionInTest where 2 in(1,2)";
        // sql = "select count(*) from ConditionInTest where id in(2)";
        sql = "select count(*) from ConditionInTest where id in(30,40,null)";
        // sql = "select count(*) from ConditionInTest where id in(1,2,2)"; //值列表允许重复

        // 跟ConditionInSelect不一样，这里没有ALL、ANY
        // sql = "select count(*) from ConditionInTest where id > ALL(1,2)";
        // ANY和SOME一样
        // sql = "select count(*) from ConditionInTest where id > ANY(1,2)";
        // sql = "select count(*) from ConditionInTest where id > SOME(1,2)";

        // sql = "select count(*) from ConditionInTest";
        printResultSet();

        // 错误，只支持一个参数
        // sql = "update ConditionInTest set name = 'abc' where id = SOME(?,?)";
        // ps = conn.prepareStatement(sql);
        // ps.setInt(1, 1);
        // ps.setInt(2, 2);
        // ps.executeUpdate();

        // sql = "update ConditionInTest set name = 'abc' where id = SOME(?)";
        // PreparedStatement ps = conn.prepareStatement(sql);
        // ps.setInt(1, 1);
        // ps.executeUpdate();
        //
        // ps.setArray(1, conn.createArrayOf(null, new Object[] { 1, 2 }));
        // ps.executeUpdate();

        // ps.addBatch();
        // ps.setInt(1, 2);
        // ps.addBatch();
        // ps.executeBatch();
    }
}
