/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.query;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DistinctQueryTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS DistinctQueryTest");
        stmt.executeUpdate(
                "create table IF NOT EXISTS DistinctQueryTest(f1 int SELECTIVITY 10, f2 int, f3 int)");
        stmt.executeUpdate("create index IF NOT EXISTS DistinctQueryTest_i1 on DistinctQueryTest(f1)");
        stmt.executeUpdate(
                "create index IF NOT EXISTS DistinctQueryTest_i1_2 on DistinctQueryTest(f1,f2)");

        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(1,3,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(5,9,3)");

        // 带查询条件的都不会触发Distinct查询优化
        sql = "select distinct * from DistinctQueryTest where f1 > 3";
        sql = "select distinct f1 from DistinctQueryTest where f1 > 3";
        int count = printResultSet();
        assertEquals(2, count);

        sql = "select distinct f1 from DistinctQueryTest";
        count = printResultSet();
        assertEquals(4, count);
        sql = "select count(distinct f1) from DistinctQueryTest";
        assertEquals(4, getIntValue(1, true));

        sql = "select distinct f1, f2 from DistinctQueryTest for update";
        count = printResultSet();
        assertEquals(6, count);
        // 不支持多个字段
        // sql = "select count(distinct f1, f2) from DistinctQueryTest";
    }
}
