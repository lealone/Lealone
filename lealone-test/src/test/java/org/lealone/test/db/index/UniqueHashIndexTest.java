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
import org.lealone.db.api.ErrorCode;

public class UniqueHashIndexTest extends IndexTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS UniqueHashIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS UniqueHashIndexTest (f1 int NOT NULL, f2 int)");

        executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS UniqueHashIndex1 ON UniqueHashIndexTest(f1)");
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
