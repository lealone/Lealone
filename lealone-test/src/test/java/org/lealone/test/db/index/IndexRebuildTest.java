/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
