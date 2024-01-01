/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.index;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;

public class UniqueHashIndexTest extends IndexTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS UniqueHashIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS UniqueHashIndexTest (f1 int NOT NULL, f2 int)");

        executeUpdate(
                "CREATE UNIQUE HASH INDEX IF NOT EXISTS UniqueHashIndex1 ON UniqueHashIndexTest(f1)");
        assertFound("UniqueHashIndexTest", "UniqueHashIndex1");

        executeUpdate("insert into UniqueHashIndexTest(f1, f2) values(1, 2)");
        executeUpdate("insert into UniqueHashIndexTest(f1, f2) values(10, 20)");
        try {
            executeUpdate("insert into UniqueHashIndexTest(f1, f2) values(10, 20)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }
        executeUpdate("insert into UniqueHashIndexTest(f1, f2) values(100, 200)");
        assertEquals(2, getInt("select count(*) from UniqueHashIndexTest where f1>=10", 1));
        printResultSet("select * from UniqueHashIndexTest where f1>=10");
        executeUpdate("delete from UniqueHashIndexTest where f1=10");
        assertEquals(2, getInt("select count(*) from UniqueHashIndexTest where f1>=1", 1));
        printResultSet("select * from UniqueHashIndexTest where f1>=1");

        executeUpdate("DROP INDEX IF EXISTS UniqueHashIndex1");
        assertNotFound("UniqueHashIndexTest", "UniqueHashIndex1");
    }
}
