/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.constraint;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;

public class ConstraintUniqueTest extends ConstraintTestBase {

    private void init() {
        executeUpdate("DROP TABLE IF EXISTS mytable");
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS mytable (f1 int not null, f2 int not null, f3 int null)");
    }

    @Test
    public void run() {
        init();
        // PRIMARY KEY也是唯一约束
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique PRIMARY KEY HASH(f1,f2)";
        executeUpdate(sql);
        assertFound("mytable", "c_unique");

        executeUpdate("insert into mytable(f1,f2) values(1, 9)");
        try {
            executeUpdate("insert into mytable(f1,f2) values(1, 9)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique");

        // 以下测试正常的唯一约束，字段没有null的情况
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique2 UNIQUE KEY INDEX(f1) NOCHECK";
        executeUpdate(sql);
        assertFound("mytable", "c_unique2");

        executeUpdate("insert into mytable(f1,f2) values(2, 9)");
        try {
            executeUpdate("insert into mytable(f1,f2) values(2, 10)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique2";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique2");

        init();
        // 以下测试正常的唯一约束，字段有null的情况
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique3 UNIQUE KEY INDEX(f3) NOCHECK";
        executeUpdate(sql);
        assertFound("mytable", "c_unique3");

        executeUpdate("insert into mytable(f1,f2,f3) values(10, 20, 30)");
        executeUpdate("insert into mytable(f1,f2,f3) values(100, 200, null)");
        executeUpdate("insert into mytable(f1,f2,f3) values(1000, 2000, 3000)");
        try {
            executeUpdate("insert into mytable(f1,f2,f3) values(10000, 20000, 30)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique3";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique3");
    }
}
