/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.query;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

// 对min、max、count三个聚合函数的特殊优化
public class QuickAggregateQueryTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS QuickAggregateQueryTest");
        stmt.executeUpdate(
                "create table IF NOT EXISTS QuickAggregateQueryTest(f1 int, f2 int, f3 int not null)");
        stmt.executeUpdate(
                "create index IF NOT EXISTS QuickAggregateQueryTest_i1 on QuickAggregateQueryTest(f1)");

        stmt.executeUpdate("insert into QuickAggregateQueryTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into QuickAggregateQueryTest(f1, f2, f3) values(1,3,3)");
        stmt.executeUpdate("insert into QuickAggregateQueryTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into QuickAggregateQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into QuickAggregateQueryTest(f1, f2, f3) values(8,2,3)");

        // f1可为null，所有不会使用QuickAggregateQuery
        sql = "select count(f1) from QuickAggregateQueryTest";
        assertEquals(5, getIntValue(1, true));

        // f3 not null，会使用QuickAggregateQuery
        sql = "select count(f3) from QuickAggregateQueryTest";
        assertEquals(5, getIntValue(1, true));

        // count(*)总是使用QuickAggregateQuery
        sql = "select count(*) from QuickAggregateQueryTest";
        assertEquals(5, getIntValue(1, true));

        sql = "select min(f1) from QuickAggregateQueryTest";
        assertEquals(1, getIntValue(1, true));

        sql = "select max(f1) from QuickAggregateQueryTest";
        assertEquals(8, getIntValue(1, true));

        // f2没有索引，所有不会使用QuickAggregateQuery
        sql = "select max(f2) from QuickAggregateQueryTest";
        assertEquals(3, getIntValue(1, true));
    }
}
