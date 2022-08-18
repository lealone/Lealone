/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ConditionInConstantSetTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS ConditionInConstantSetTest");
        stmt.executeUpdate(
                "create table IF NOT EXISTS ConditionInConstantSetTest(id int, name varchar(50))");

        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into ConditionInConstantSetTest(id, name) values(3, 'b3')");

        // 在Parser.readCondition()里直接解析成ValueExpression(false)
        sql = "select count(*) from ConditionInConstantSetTest where id in()";
        assertEquals(0, getIntValue(1, true));
        sql = "select count(*) from ConditionInConstantSetTest where id in(3,2)";
        assertEquals(4, getIntValue(1, true));
        // 后面的id = 1会进行优化，直接加了in列表中
        sql = "select count(*) from ConditionInConstantSetTest where id in(3,2) or id = 1";
        assertEquals(6, getIntValue(1, true));
    }
}
