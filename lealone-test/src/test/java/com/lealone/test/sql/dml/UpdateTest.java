/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class UpdateTest extends SqlTestBase {
    @Test
    public void run() {
        createTable("UpdateTest");
        testInsert();
        testUpdate();
        testUpdatePrimaryKey();
        testUpdateIndex();
    }

    void testUpdatePrimaryKey() {
        executeUpdate("DROP TABLE IF EXISTS testUpdatePrimaryKey");
        executeUpdate("CREATE TABLE testUpdatePrimaryKey (pk int PRIMARY KEY, f1 int)");
        executeUpdate("INSERT INTO testUpdatePrimaryKey(pk, f1) VALUES(1, 10)");
        executeUpdate("INSERT INTO testUpdatePrimaryKey(pk, f1) VALUES(2, 20)");
        sql = "UPDATE testUpdatePrimaryKey SET pk=3 WHERE pk = 1";
        assertEquals(1, executeUpdate(sql));
        sql = "UPDATE testUpdatePrimaryKey SET pk=2 WHERE pk = 2";
        assertEquals(1, executeUpdate(sql));
    }

    void testUpdateIndex() {
        executeUpdate("DROP TABLE IF EXISTS testUpdateIndex");
        executeUpdate("CREATE TABLE testUpdateIndex (pk int PRIMARY KEY, f1 int, f2 int)");
        executeUpdate("CREATE INDEX i_f2 ON testUpdateIndex(f2)");
        executeUpdate("INSERT INTO testUpdateIndex(pk, f1, f2) VALUES(1, 10, 100)");
        executeUpdate("INSERT INTO testUpdateIndex(pk, f1, f2) VALUES(2, 20, 200)");
        sql = "UPDATE testUpdateIndex SET f1=11 WHERE pk = 1";
        assertEquals(1, executeUpdate(sql));
        sql = "UPDATE testUpdateIndex SET f2=201 WHERE pk = 2";
        assertEquals(1, executeUpdate(sql));
        sql = "SELECT f2 FROM testUpdateIndex WHERE pk = 2";
        try {
            assertEquals(201, getIntValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 测试执行SQL的调度器跟执行页面操作的调度器之间的协作
        for (int i = 100; i < 300; i++) {
            executeUpdate("INSERT INTO testUpdateIndex(pk, f1, f2) VALUES(" + i + ", 10, 100)");
        }
        sql = "SELECT count(*) FROM testUpdateIndex WHERE pk >= 100";
        try {
            assertEquals(200, getIntValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void testInsert() {
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO UpdateTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testUpdate() {
        sql = "UPDATE UpdateTest SET f1 = 'a1', f3 = 61+LENGTH(f2) WHERE pk = '01'";
        assertEquals(1, executeUpdate(sql));

        try {
            sql = "SELECT f1, f2, f3 FROM UpdateTest WHERE pk = '01'";
            assertEquals("a1", getStringValue(1));
            assertEquals("b", getStringValue(2));
            assertEquals(62, getIntValue(3, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
