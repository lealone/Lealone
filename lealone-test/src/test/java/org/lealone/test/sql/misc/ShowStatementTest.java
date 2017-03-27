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
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class ShowStatementTest extends SqlTestBase {

    public ShowStatementTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_client_server RUN MODE client_server");
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_replication RUN MODE replication");
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_sharding RUN MODE sharding");

        ShowStatementTest t = new ShowStatementTest();
        t.dbName = DEFAULT_DB_NAME;
        t.runTest();

        test();
    }

    @Override
    protected void test() throws Exception {
        p("dbName=" + dbName);
        p("--------------------");
        sql = "show schemas";
        printResultSet();

        sql = "show databases";
        printResultSet();

        sql = "select * from information_schema.databases";
        printResultSet();

        sql = "select count(*) from information_schema.databases";
        if (dbName.equals(LealoneDatabase.NAME))
            assertTrue(getIntValue(1, true) >= (4 + 1)); // 至少有5个数据库
        else
            assertEquals(1, getIntValue(1, true));
    }

}
