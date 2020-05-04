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
