/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import org.junit.Test;

import com.lealone.db.LealoneDatabase;
import com.lealone.test.sql.SqlTestBase;

// 执行insert语句时会从TableAlterHistory获取表结构的版本号，
// TableAlterHistory通过执行Prepared类型的select语句查询版本号，
// 当Prepared类型的select语句被执行多次后会让出执行权可能会导致insert语句被执行两次
public class SchedulerYieldBugTest extends SqlTestBase {

    public SchedulerYieldBugTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        // bug();
    }

    public void bug() throws Exception {
        for (int t = 1; t <= 100; t++) {
            String tableName = "SchedulerYieldBugTest_" + t;
            executeUpdate("drop table IF EXISTS " + tableName);
            executeUpdate(
                    "create table IF NOT EXISTS " + tableName + "(f1 int primary key, f2 int, f3 int)");

            for (int i = 1; i <= 100; i++) {
                String sql = "insert into " + tableName + "(f1, f2, f3) values(" + i + "," + i + "," + i
                        + ")";
                executeUpdate(sql);
            }
        }
    }
}
