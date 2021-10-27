/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExpressionListTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testExpressionList();
    }

    void init() throws Exception {
        createTable("ExpressionListTest");

        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('01', 'a1', 10)");
        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('02', 'a2', 50)");
        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('03', 'a3', 30)");

        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('04', 'a4', 40)");
        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('05', 'a5', 20)");
        executeUpdate("INSERT INTO ExpressionListTest(pk, f1, f2) VALUES('06', 'a6', 60)");
    }

    void testExpressionList() throws Exception {
        sql = "SELECT count(*) FROM ExpressionListTest WHERE (f1,f2) >= (10)";
        assertEquals(6, getIntValue(1, true));
        sql = "SELECT count(*) FROM ExpressionListTest WHERE (f1,f2) >= ('a2', 10)";
        assertEquals(5, getIntValue(1, true));
    }
}
