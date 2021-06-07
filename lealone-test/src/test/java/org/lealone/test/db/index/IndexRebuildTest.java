/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.index;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.db.index.Index;

public class IndexRebuildTest extends IndexTestBase {
    @Test
    public void run() {
        executeUpdate("set MAX_MEMORY_ROWS 4");

        executeUpdate("DROP TABLE IF EXISTS IndexRebuildTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS IndexRebuildTest (f1 int, f2 int)");

        executeUpdate("insert into IndexRebuildTest(f1, f2) values(1, 2)");
        executeUpdate("insert into IndexRebuildTest(f1, f2) values(10, 20)");
        executeUpdate("insert into IndexRebuildTest(f1, f2) values(100, 200)");
        executeUpdate("insert into IndexRebuildTest(f1, f2) values(10000, 2000)");
        executeUpdate("insert into IndexRebuildTest(f1, f2) values(100000, 20000)");

        executeUpdate(
                "CREATE UNIQUE HASH INDEX IF NOT EXISTS IndexRebuildTest_UniqueHashIndex1 ON IndexRebuildTest(f1)");
        executeUpdate("CREATE HASH INDEX IF NOT EXISTS IndexRebuildTest_NonUniqueHashIndex1 ON IndexRebuildTest(f1)");
        executeUpdate("CREATE INDEX IF NOT EXISTS IndexRebuildTest_StandardIndex1 ON IndexRebuildTest(f1)");

        assertFound("IndexRebuildTest", "IndexRebuildTest_UniqueHashIndex1");
        assertFound("IndexRebuildTest", "IndexRebuildTest_NonUniqueHashIndex1");
        assertFound("IndexRebuildTest", "IndexRebuildTest_StandardIndex1");

        Index index = getIndex("IndexRebuildTest_UniqueHashIndex1");
        assertEquals(5, index.getRowCount(session));
        index = getIndex("IndexRebuildTest_NonUniqueHashIndex1");
        assertEquals(5, index.getRowCount(session));
        index = getIndex("IndexRebuildTest_StandardIndex1");
        assertEquals(5, index.getRowCount(session));

        // 恢复到默认值，避免影响其他测试用例
        executeUpdate("set MAX_MEMORY_ROWS " + Constants.DEFAULT_MAX_MEMORY_ROWS);
    }
}
