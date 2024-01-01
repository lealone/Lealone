/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.constraint;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;

public class ConstraintCheckTest extends ConstraintTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS mytable");
        executeUpdate("CREATE TABLE IF NOT EXISTS mytable (f1 int, f2 int not null)");

        // 第一个CHECK表示这是一个CHECK约束，
        // 第二个CHECK指示在执行这条ALTER语句后马上对现有记录做检查，如果不满足条件就不允许创建
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_check CHECK f1>0 and f2<10 CHECK";
        executeUpdate(sql);
        assertFound("mytable", "c_check");

        executeUpdate("insert into mytable(f1,f2) values(1, 9)"); // ok
        try {
            executeUpdate("insert into mytable(f1,f2) values(-1, 9)"); // 不满足f1>0
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.CHECK_CONSTRAINT_VIOLATED_1);
        }
        try {
            executeUpdate("insert into mytable(f1,f2) values(2, 10)"); // 不满足f2<10
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.CHECK_CONSTRAINT_VIOLATED_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_check";
        executeUpdate(sql);
        assertNotFound("mytable", "c_check");

        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_check2 CHECK f2<8 CHECK";
        try {
            executeUpdate(sql); // 已经有一条记录并且f2的值为9，不满足f2<8
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.CHECK_CONSTRAINT_VIOLATED_1);
        }
    }
}
