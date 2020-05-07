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
package org.lealone.test.sql.index;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class AsyncIndexTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("set MAX_MEMORY_ROWS 10");
        stmt.executeUpdate("DROP TABLE IF EXISTS AsyncIndexTest");
        // stmt.executeUpdate("CREATE local temporary TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3
        // varchar)");
        // stmt.executeUpdate("CREATE global temporary TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3
        // varchar)");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS AsyncIndexTest (f1 int NOT NULL, f2 int, f3 varchar)");

        for (int i = 100; i < 200; i++) {
            String sql = "INSERT INTO AsyncIndexTest(f1, f2, f3) VALUES(" + i + "," + (i * 10) + ",'a" + i + "')";
            stmt.executeUpdate(sql);
        }

        stmt.executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS AsyncIndexTest_idx0 ON AsyncIndexTest(f1)");
        stmt.executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS AsyncIndexTest_idx1 ON AsyncIndexTest(f2)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS AsyncIndexTest_idx2 ON AsyncIndexTest(f3, f2)");

        stmt.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS AsyncIndexTest_idx3 ON AsyncIndexTest(f2, f3)");
    }
}
