/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import java.sql.PreparedStatement;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;
import com.lealone.test.sql.SqlTestBase;

public class MergeTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testPrimaryKey();
        testFromSelect();
        testFromValues();
        testOthers();
    }

    private void init() {
        executeUpdate("DROP TABLE IF EXISTS tmpSelectTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS tmpSelectTest(id int, name varchar(500))");
        executeUpdate("DROP TABLE IF EXISTS MergeTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS MergeTest(id int, name varchar(500))");
    }

    void testPrimaryKey() {
        executeUpdate("DROP TABLE IF EXISTS testPrimaryKey");
        // 如果id是primary key，那么在MERGE语句中KEY子句可省，默认用primary key
        sql = "CREATE TABLE IF NOT EXISTS testPrimaryKey(id int primary key, f1 int)";
        executeUpdate(sql);
        sql = "MERGE INTO testPrimaryKey VALUES(20, 2)";
        assertEquals(1, executeUpdate(sql));

        executeUpdate("DROP TABLE IF EXISTS testPrimaryKey");
        sql = "CREATE TABLE IF NOT EXISTS testPrimaryKey(id int, f1 int)";
        executeUpdate(sql);
        // id不是primary key，需要把KEY子句加上
        sql = "MERGE INTO testPrimaryKey VALUES(20, 2)";
        executeUpdateThanAssertErrorCode(sql, ErrorCode.CONSTRAINT_NOT_FOUND_1);

        // ok
        sql = "MERGE INTO testPrimaryKey KEY(id) VALUES(20, 2)";
        assertEquals(1, executeUpdate(sql));
    }

    void testFromSelect() {
        sql = "INSERT INTO tmpSelectTest VALUES(DEFAULT, DEFAULT),(10, 'a'),(20, 'b')";
        // sql = "INSERT INTO tmpSelectTest VALUES(DEFAULT, 'c'),(10, 'a'),(20, 'b')";
        assertEquals(3, executeUpdate(sql));

        // 从另一张表查数据，然后插入此表
        sql = "MERGE INTO MergeTest KEY(id) (SELECT * FROM tmpSelectTest)";
        assertEquals(3, executeUpdate(sql));
    }

    void testFromValues() {
        // 这种语法可插入多条记录
        // 30 null
        // 10 a
        // 20 b
        sql = "MERGE INTO MergeTest KEY(id) VALUES(30, DEFAULT),(10, 'a'),(20, 'b')";
        assertEquals(3, executeUpdate(sql));

        sql = "MERGE INTO MergeTest(name, id) KEY(id) VALUES('abc', 10)";
        assertEquals(1, executeUpdate(sql));
        try {
            sql = "SELECT name FROM MergeTest WHERE id=10";
            assertEquals("abc", getStringValue(1, true));

            PreparedStatement ps = conn
                    .prepareStatement("MERGE INTO MergeTest(id, name) KEY(id) VALUES(?, ?)");
            ps.setInt(1, 30);
            ps.setString(2, "c");
            ps.executeUpdate();
            ps.close();

            sql = "SELECT name FROM MergeTest WHERE id=30";
            assertEquals("c", getStringValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        sql = "EXPLAIN MERGE INTO MergeTest(id, name) KEY(id) SELECT * FROM tmpSelectTest";
        printResultSet();
    }

    void testOthers() throws Exception {
        // 允许: INSERT INTO MergeTest VALUES()
        // 但是不允许: MERGE INTO MergeTest KEY(id) VALUES()
        assertEquals(1, executeUpdate("INSERT INTO MergeTest VALUES()"));
        sql = "MERGE INTO MergeTest KEY(id) VALUES()";
        executeUpdateThanAssertErrorCode(sql, ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1);

        // KEY子句中的字段值不能为null，id的字段类型是允许为null的，用DEFAULT就是用null值
        sql = "MERGE INTO MergeTest KEY(id) VALUES(DEFAULT, DEFAULT),(10, 'a'),(20, 'b')";
        executeUpdateThanAssertErrorCode(sql, ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1);

        // key字段必须出现在VALUES中
        sql = "MERGE INTO MergeTest(name) KEY(id) VALUES('abc')";
        executeUpdateThanAssertErrorCode(sql, ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1);

        // 列必须一样多，否则:com.lealone.message.JdbcSQLException: Column count does not match;
        sql = "MERGE INTO MergeTest(name) KEY(id) (SELECT * FROM tmpSelectTest)";
        executeUpdateThanAssertErrorCode(sql, ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
    }
}
