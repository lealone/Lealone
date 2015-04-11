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
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.test.TestBase;

public class CreateIndexTest extends TestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS CreateIndexTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CreateIndexTest (f1 int NOT NULL, f2 int, f3 int)");

        //stmt.executeUpdate("CREATE PRIMARY KEY HASH ON CreateIndexTest(f1)");
        stmt.executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS idx0 ON CreateIndexTest(f1)");

        stmt.executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS idx1 ON CreateIndexTest(f2)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx2 ON CreateIndexTest(f3)");

        stmt.executeUpdate("ALTER INDEX idx2 RENAME TO idx22");

        stmt.executeUpdate("DROP INDEX IF EXISTS idx22");

        //stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS schema0 AUTHORIZATION sa");
        //stmt.executeUpdate("ALTER INDEX mydb.public.idx0 RENAME TO schema0.idx1");

        //stmt.executeUpdate("ALTER INDEX mydb.public.idx0 RENAME TO idx1");

    }

}
