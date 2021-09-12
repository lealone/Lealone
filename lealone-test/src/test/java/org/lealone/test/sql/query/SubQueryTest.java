/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.query;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class SubQueryTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testSelect();
    }

    void init() throws Exception {
        createTable("SubQueryTest");

        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('01', 'a1', 10)");
        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('02', 'a2', 50)");
        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('03', 'a3', 30)");

        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('04', 'a4', 40)");
        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('05', 'a5', 20)");
        executeUpdate("INSERT INTO SubQueryTest(pk, f1, f2) VALUES('06', 'a6', 60)");
    }

    void testSelect() throws Exception {
        // scalar subquery
        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND f2 >= (SELECT f2 FROM SubQueryTest WHERE pk='01')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND EXISTS(SELECT f2 FROM SubQueryTest WHERE pk='01' AND f1='a1')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND f2 IN(SELECT f2 FROM SubQueryTest WHERE pk>='04')";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND f2 < ALL(SELECT f2 FROM SubQueryTest WHERE pk>='04')";
        assertEquals(1, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND f2 < ANY(SELECT f2 FROM SubQueryTest WHERE pk>='04')";
        assertEquals(5, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubQueryTest WHERE pk>='01'" //
                + " AND f2 < SOME(SELECT f2 FROM SubQueryTest WHERE pk>='04')";
        assertEquals(5, getIntValue(1, true));
    }
}
