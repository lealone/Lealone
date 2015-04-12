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
import org.lealone.test.sql.TestBase;

public class AlterIndexRenameTest extends TestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS AlterIndexRenameTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS AlterIndexRenameTest (f1 int)");

        executeUpdate("CREATE INDEX IF NOT EXISTS AlterIndexRenameTest_idx0 ON AlterIndexRenameTest(f1)");

        //executeUpdate("ALTER INDEX AlterIndexRenameTest_idx0 RENAME TO AlterIndexRenameTest_idx1");

        //executeUpdate("CREATE SCHEMA IF NOT EXISTS schema0 AUTHORIZATION sa");
        //executeUpdate("ALTER INDEX mydb.public.AlterIndexRenameTest_idx0 RENAME TO schema0.AlterIndexRenameTest_idx1");

        executeUpdate("ALTER INDEX public.AlterIndexRenameTest_idx0 RENAME TO AlterIndexRenameTest_idx1");
    }
}
