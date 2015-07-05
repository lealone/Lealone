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
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class BackupCommandTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS BackupCommandTest");
        executeUpdate("create table IF NOT EXISTS BackupCommandTest(id int, name varchar(500), b boolean)");
        executeUpdate("CREATE INDEX IF NOT EXISTS BackupCommandTestIndex ON BackupCommandTest(name)");

        executeUpdate("insert into BackupCommandTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into BackupCommandTest(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into BackupCommandTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into BackupCommandTest(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into BackupCommandTest(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into BackupCommandTest(id, name, b) values(3, 'b3', true)");

        sql = "BACKUP TO " + TEST_DIR + "/myBackup.zip"; // 文件名要加单引号
        sql = "BACKUP TO '" + TEST_DIR + "/myBackup.zip'";
        executeUpdate(sql);

        sql = "select * from BackupCommandTest";
        printResultSet();
    }
}
