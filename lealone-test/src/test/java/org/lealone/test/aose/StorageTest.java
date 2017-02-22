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
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class StorageTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("CREATE TABLE IF NOT EXISTS StorageTest(f1 int, f2 int) ENGINE " + DEFAULT_STORAGE_ENGINE_NAME
                + " WITH(map_type=BufferedMap)");
        executeUpdate("INSERT INTO StorageTest(f1, f2) VALUES(1, 10)");
        executeUpdate("INSERT INTO StorageTest(f1, f2) VALUES(2, 20)");

        sql = "SELECT * FROM StorageTest";
        printResultSet();
    }
}
