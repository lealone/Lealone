/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class RownumTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testRownum();
    }

    void init() throws Exception {
        createTable("RownumTest");

        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('01', 'a1', 10)");
        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('02', 'a2', 50)");
        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('03', 'a3', 30)");

        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('04', 'a4', 40)");
        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('05', 'a5', 20)");
        executeUpdate("INSERT INTO RownumTest(pk, f1, f2) VALUES('06', 'a6', 60)");
    }

    void testRownum() throws Exception {
        sql = "SELECT ROWNUM,pk FROM RownumTest";
        assertEquals(1, getIntValue(1, true));
        sql = "SELECT ROW_NUMBER()OVER(),pk FROM RownumTest";
        assertEquals(1, getIntValue(1, true));
    }
}
