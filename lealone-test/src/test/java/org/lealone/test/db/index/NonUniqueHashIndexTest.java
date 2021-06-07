/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.index;

import org.junit.Test;

public class NonUniqueHashIndexTest extends IndexTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS NonUniqueHashIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS NonUniqueHashIndexTest (f1 int NOT NULL, f2 int)");

        executeUpdate("CREATE HASH INDEX IF NOT EXISTS NonUniqueHashIndex1 ON NonUniqueHashIndexTest(f1)");
        assertFound("NonUniqueHashIndexTest", "NonUniqueHashIndex1");

        executeUpdate("insert into NonUniqueHashIndexTest(f1, f2) values(1, 2)");
        executeUpdate("insert into NonUniqueHashIndexTest(f1, f2) values(10, 20)");
        executeUpdate("insert into NonUniqueHashIndexTest(f1, f2) values(10, 20)"); // ok
        executeUpdate("insert into NonUniqueHashIndexTest(f1, f2) values(100, 200)");

        assertEquals(3, getInt("select count(*) from NonUniqueHashIndexTest where f1>=10", 1));
        printResultSet("select * from NonUniqueHashIndexTest where f1>=10");
        executeUpdate("delete from NonUniqueHashIndexTest where f1=10");
        assertEquals(2, getInt("select count(*) from NonUniqueHashIndexTest where f1>=1", 1));
        printResultSet("select * from NonUniqueHashIndexTest where f1>=1");

        executeUpdate("DROP INDEX IF EXISTS NonUniqueHashIndex1");
        assertNotFound("NonUniqueHashIndexTest", "NonUniqueHashIndex1");
    }
}
