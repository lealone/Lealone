/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.index;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.test.sql.SqlTestBase;

public class AsyncIndexTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("set MAX_MEMORY_ROWS 60");

        stmt.executeUpdate("DROP TABLE IF EXISTS AsyncIndexTest");
        // stmt.executeUpdate("CREATE local temporary TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3
        // varchar)");
        // stmt.executeUpdate("CREATE global temporary TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3
        // varchar)");
        stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3 varchar)");

        for (int i = 100; i < 200; i++) {
            String sql = "INSERT INTO AsyncIndexTest(f1, f2, f3) VALUES(" + i + "," + (i * 10) + ",'a"
                    + i + "')";
            stmt.executeUpdate(sql);
        }

        stmt.executeUpdate(
                "CREATE PRIMARY KEY HASH IF NOT EXISTS AsyncIndexTest_idx0 ON AsyncIndexTest(f1)");
        stmt.executeUpdate(
                "CREATE UNIQUE HASH INDEX IF NOT EXISTS AsyncIndexTest_idx1 ON AsyncIndexTest(f2)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS AsyncIndexTest_idx2 ON AsyncIndexTest(f3, f2)");

        stmt.executeUpdate(
                "CREATE UNIQUE INDEX IF NOT EXISTS AsyncIndexTest_idx3 ON AsyncIndexTest(f2, f3)");

        // 恢复到默认值，避免影响其他测试用例
        stmt.executeUpdate("set MAX_MEMORY_ROWS " + Constants.DEFAULT_MAX_MEMORY_ROWS);
    }
}
