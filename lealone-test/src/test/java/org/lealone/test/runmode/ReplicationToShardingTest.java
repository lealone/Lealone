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
package org.lealone.test.runmode;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ReplicationToShardingTest extends RunModeTest {

    public ReplicationToShardingTest() {
        setHost("127.0.0.1");
    }

    @Test
    @Override
    public void run() throws Exception {
        String dbName = ReplicationToShardingTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE replication " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        new CrudTest(dbName).runTest();

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, nodes: 2)");
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
            batch();
        }

        void insert() {
            executeUpdate("drop table IF EXISTS test");
            executeUpdate("create table IF NOT EXISTS test(f1 int SELECTIVITY 10, f2 int, f3 int)");
            // executeUpdate("create index IF NOT EXISTS test_i1 on test(f1)");

            executeUpdate("insert into test(f1, f2, f3) values(1,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(5,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(3,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(8,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(3,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(8,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(3,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(8,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(3,2,3)");
            executeUpdate("insert into test(f1, f2, f3) values(8,2,3)");
        }

        void select() {
            sql = "select distinct * from test where f1 > 3";
            sql = "select distinct f1 from test";
            printResultSet();
        }

        void batch() {
            int count = 35;
            for (int i = 0; i < count; i++) {
                String tableName = "run_mode_test_" + i;
                executeUpdate("create table IF NOT EXISTS " + tableName + "(f0 int, f1 int, f2 int, f3 int, f4 int,"
                        + " f5 int, f6 int, f7 int, f8 int, f9 int)");
                int rows = 50;
                StringBuilder sql = new StringBuilder();
                for (int j = 0; j < rows; j++) {
                    sql.append("insert into " + tableName + " values(0,1,2,3,4,5,6,7,8,9);");
                    // executeUpdate("insert into " + tableName + " values(0,1,2,3,4,5,6,7,8,9)");
                }
                sql.setLength(sql.length() - 1);
                executeUpdate(sql.toString());
            }
        }
    }
}
