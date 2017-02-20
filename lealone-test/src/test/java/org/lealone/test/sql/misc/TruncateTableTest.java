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
package org.lealone.test.sql.misc;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class TruncateTableTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS TruncateTableTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS TruncateTableTest (f1 int,f2 int)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(1,3)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(2,1)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(3,2)");
        executeUpdate("CREATE INDEX IF NOT EXISTS TruncateTableTest_idx2 ON TruncateTableTest(f2)");
        executeUpdate("TRUNCATE TABLE TruncateTableTest");
    }

}
