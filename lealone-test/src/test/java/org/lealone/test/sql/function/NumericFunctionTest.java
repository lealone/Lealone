/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.function;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class NumericFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();

        sql = "SELECT abs(f3) FROM NumericFunctionTest WHERE pk = '01'";
        assertEquals(1, getIntValue(1, true));
    }

    void init() throws Exception {
        createTable("NumericFunctionTest");

        // 在分区1中保存1到11中的奇数
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', -1)");
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 3)");
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 5)");
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('04', 'a2', 'b', 7)");
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('05', 'a2', 'b', 9)");
        executeUpdate("INSERT INTO NumericFunctionTest(pk, f1, f2, f3) VALUES('06', 'a2', 'b', 11)");
    }
}
