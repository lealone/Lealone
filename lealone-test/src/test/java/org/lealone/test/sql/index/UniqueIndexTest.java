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

public class UniqueIndexTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS UniqueIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS UniqueIndexTest (f1 int NOT NULL, f2 int, f3 varchar)");

        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(100, 10, 'a')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(200, 20, 'b')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(300, 30, 'c')");

        executeUpdate("SET MAX_MEMORY_ROWS 2");
        executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS UniqueIndexTest_ui ON UniqueIndexTest(f2, f3)");
        try {
            executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(400, 20, 'b')");
            fail("insert duplicate key: 20");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
