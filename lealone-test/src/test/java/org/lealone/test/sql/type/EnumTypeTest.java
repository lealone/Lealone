/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.type;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class EnumTypeTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("drop table if exists EnumTypeTest");
        executeUpdate(
                "create table if not exists EnumTypeTest (f1 int, f2 ENUM ('sad', 'ok', 'happy'))");
        executeUpdate("create index i_EnumTypeTest on EnumTypeTest(f2)");

        executeUpdate("insert into EnumTypeTest values(10, 'ok')");
        executeUpdate("insert into EnumTypeTest values(11, 3)");

        sql = "SELECT f2 FROM EnumTypeTest where f1=10";
        assertEquals(2, getIntValue(1, true));

        sql = "SELECT f2 FROM EnumTypeTest where f1=11";
        assertEquals("happy", getStringValue(1, true));

        // 按索引查
        sql = "SELECT f1 FROM EnumTypeTest where f2='happy'";
        assertEquals(11, getIntValue(1, true));
        sql = "SELECT f1 FROM EnumTypeTest where f2=3";
        assertEquals(11, getIntValue(1, true));
    }
}
