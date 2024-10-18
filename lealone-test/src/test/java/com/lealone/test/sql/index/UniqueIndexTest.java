/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.index;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;
import com.lealone.test.sql.SqlTestBase;

public class UniqueIndexTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS UniqueIndexTest");
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS UniqueIndexTest (f1 int NOT NULL, f2 int, f3 varchar)");
        executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS UniqueIndexTest_ui ON UniqueIndexTest(f2, f3)");

        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(100, 10, 'a')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(200, 20, 'b')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(300, 30, 'c')");

        try {
            executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(400, 20, 'b')");
            fail("insert duplicate key: 20");
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.DUPLICATE_KEY_1);
        }
    }
}
