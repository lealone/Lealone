/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExpressionColumnTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS ExpressionColumnTest");
        stmt.executeUpdate("create table IF NOT EXISTS ExpressionColumnTest(id int, name varchar(500))");

        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(1, 'a1')");
        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(1, 'b1')");
        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(2, 'a2')");
        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(2, 'b2')");
        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(3, 'a3')");
        stmt.executeUpdate("insert into ExpressionColumnTest(id, name) values(3, 'b3')");

        // public.ExpressionColumnTest.id这样用是不对的
        // 在org.h2.expression.ExpressionColumn.optimize(Session)中检查
        sql = "SELECT public.ExpressionColumnTest.id FROM ExpressionColumnTest as t";
        // public.t.id这样才行，因为右边有as t
        sql = "SELECT public.t.id FROM ExpressionColumnTest as t";
        sql = "SELECT _rowid_, id, name FROM ExpressionColumnTest WHERE _rowid_>2";

        assertEquals(4, printResultSet());
    }
}
