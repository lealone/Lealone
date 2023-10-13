/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import org.junit.Test;
import org.lealone.common.trace.TraceSystem;
import org.lealone.test.sql.SqlTestBase;

public class QueryStatisticsTest extends SqlTestBase {

    public QueryStatisticsTest() {
        super("QueryStatisticsTestDB");
        addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", TraceSystem.INFO);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("set QUERY_STATISTICS 1");
        stmt.executeUpdate("set QUERY_STATISTICS_MAX_ENTRIES 200");
        insert();
        select();
    }

    void insert() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS QueryStatisticsTest");
        stmt.executeUpdate("create table IF NOT EXISTS QueryStatisticsTest(f1 int, f2 int, f3 int)");
        stmt.executeUpdate("insert into QueryStatisticsTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into QueryStatisticsTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into QueryStatisticsTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into QueryStatisticsTest(f1, f2, f3) values(8,2,3)");
    }

    void select() throws Exception {
        sql = "select distinct * from QueryStatisticsTest where f1 > 3";
        sql = "select distinct f1 from QueryStatisticsTest";
        printResultSet();

        sql = "select * from PERFORMANCE_SCHEMA.QUERY_STATISTICS";
        printResultSet();
    }

}
