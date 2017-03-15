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
package org.lealone.test.misc;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class RunModeTest extends SqlTestBase {

    public RunModeTest() {
        super(LealoneDatabase.NAME); // 连到LealoneDatabase才能执行CREATE DATABASE
    }

    @Test
    public void run() throws Exception {
        String dbName = "RunModeTestDB1";
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE client_server");
        // new CrudTest(dbName).runTest();

        dbName = "RunModeTestDB2";
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE replication");
        new CrudTest(dbName).runTest();

        dbName = "RunModeTestDB3";
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding");
        // new CrudTest(dbName).runTest();

        // String p = " PARAMETERS(hostIds='1,2')";
        // stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding" + p);
    }

    private static class CrudTest extends SqlTestBase {

        public CrudTest(String dbName) {
            super(dbName);
            // setHost("127.0.0.1"); //可以测试localhost和127.0.0.1是否复用了同一个TCP连接
        }

        @Override
        protected void test() throws Exception {
            insert();
            select();
        }

        void insert() throws Exception {
            stmt.executeUpdate("drop table IF EXISTS RunModeTest");
            stmt.executeUpdate("create table IF NOT EXISTS RunModeTest(f1 int SELECTIVITY 10, f2 int, f3 int)");

            // stmt.executeUpdate("create index IF NOT EXISTS RunModeTest_i1 on RunModeTest(f1)");

            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(1,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(5,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into RunModeTest(f1, f2, f3) values(8,2,3)");
        }

        void select() throws Exception {
            sql = "select distinct * from RunModeTest where f1 > 3";
            sql = "select distinct f1 from RunModeTest";
            printResultSet();
        }
    }
}
