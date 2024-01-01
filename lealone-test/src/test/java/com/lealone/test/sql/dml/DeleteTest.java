/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class DeleteTest extends SqlTestBase {
    @Test
    public void run() {
        createTable("DeleteTest");
        testInsert();
        testDelete();
    }

    void testInsert() {
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testDelete() {
        sql = "DELETE FROM DeleteTest WHERE pk = '01'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk <= '25'";
        assertEquals(3, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk = '26'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk > '25' AND pk < '50'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk >= '50'";
        assertEquals(6, executeUpdate(sql));

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('101', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('102', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('103', 'a1', 'b', 12)");

        sql = "DELETE TOP 1 FROM DeleteTest";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest LIMIT 2";
        assertEquals(2, executeUpdate(sql));
    }
}
