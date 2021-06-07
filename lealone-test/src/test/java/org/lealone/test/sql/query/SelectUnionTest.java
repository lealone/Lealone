/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.query;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.test.sql.SqlTestBase;

public class SelectUnionTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        init();
        testUnion();
        testIntersect();
        testExcept();
    }

    void init() {
        executeUpdate("DROP TABLE IF EXISTS SelectUnionTest1");
        executeUpdate("CREATE TABLE IF NOT EXISTS SelectUnionTest1(id int, name varchar(500), b boolean, id1 int)");
        executeUpdate("CREATE INDEX IF NOT EXISTS SelectUnionTestIndex1 ON SelectUnionTest1(name)");

        executeUpdate("DROP TABLE IF EXISTS SelectUnionTest2");
        executeUpdate("CREATE TABLE IF NOT EXISTS SelectUnionTest2(id int, name varchar(500), b boolean, id2 int)");
        executeUpdate("CREATE INDEX IF NOT EXISTS SelectUnionTestIndex2 ON SelectUnionTest2(name)");

        executeUpdate("insert into SelectUnionTest1(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(3, 'b3', true)");

        executeUpdate("insert into SelectUnionTest2(id, name, b) values(4, 'a1', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(4, 'b1', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(5, 'a2', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(5, 'b2', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(6, 'a3', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(6, 'b3', true)");

        executeUpdate("insert into SelectUnionTest2(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(3, 'b3', true)");
    }

    void testUnion() throws Exception {
        // 查询列个数必须一样
        sql = "select id from SelectUnionTest1 UNION select name, b from SelectUnionTest2";
        executeQueryThanAssertErrorCode(sql, ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);

        // 查询列名类型必须能转换，右边的name为字符串类型，转换会出错
        sql = "select id from SelectUnionTest1 UNION select name from SelectUnionTest2";
        executeQueryThanAssertErrorCode(sql, ErrorCode.DATA_CONVERSION_ERROR_1);

        // 这里只以id字段去重，所以只有6条
        sql = "select count(*) from (select id from SelectUnionTest1 UNION select id from SelectUnionTest2)";
        assertEquals(6, getIntValue(1, true));

        // 这两条是等价的，没有重复
        // 这里以所有字段去重，所以有12条，去掉最后两条
        sql = "select count(*) from (select * from SelectUnionTest1 UNION select * from SelectUnionTest2)";
        assertEquals(12, getIntValue(1, true));
        sql = "select count(*) from (select * from SelectUnionTest1 UNION DISTINCT select * from SelectUnionTest2)";
        assertEquals(12, getIntValue(1, true));

        // 包含所有记录
        sql = "select count(*) from (select * from SelectUnionTest1 UNION ALL select * from SelectUnionTest2)";
        assertEquals(14, getIntValue(1, true));
    }

    void testIntersect() throws Exception {
        // 按所有字段求交集，只有两条
        sql = "select count(*) from (select * from SelectUnionTest1 INTERSECT select * from SelectUnionTest2)";
        assertEquals(2, getIntValue(1, true));
    }

    void testExcept() throws Exception {
        // 这两条是等价的，结果集有重复
        sql = "select count(*) from (select * from SelectUnionTest1 EXCEPT select * from SelectUnionTest2)";
        assertEquals(4, getIntValue(1, true));
        sql = "select count(*) from (select * from SelectUnionTest1 MINUS select * from SelectUnionTest2)";
        assertEquals(4, getIntValue(1, true));
    }
}
