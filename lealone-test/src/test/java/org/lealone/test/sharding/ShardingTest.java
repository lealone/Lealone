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
package org.lealone.test.sharding;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class ShardingTest extends SqlTestBase {

    public ShardingTest() {
        super(LealoneDatabase.NAME); // 连到LealoneDatabase才能执行CREATE DATABASE
    }

    @Test
    public void run() throws Exception {
        String dbName = "ShardingTestDB";

        sql = "CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding";
        sql += " PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1,";
        sql += " node_assignment_strategy: 'RandomNodeAssignmentStrategy', assignment_factor: 2)";
        stmt.executeUpdate(sql);

        new TwoTablesTest(dbName).runTest();
        // new ShardingFindTest(dbName).runTest();
        // new ShardingCrudTest(dbName).runTest();
    }

    class TwoTablesTest extends SqlTestBase {

        public TwoTablesTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            executeUpdate("drop table IF EXISTS TwoTablesTest1");
            executeUpdate("create table IF NOT EXISTS TwoTablesTest1(f1 int primary key, f2 int)");

            executeUpdate("drop table IF EXISTS TwoTablesTest2");
            executeUpdate("create table IF NOT EXISTS TwoTablesTest2(f1 int primary key, f2 int)");

            conn.setAutoCommit(false);
            executeUpdate("insert into TwoTablesTest1(f1, f2) values(10, 20)");
            executeUpdate("insert into TwoTablesTest2(f1, f2) values(10, 20)");
            conn.commit();
        }
    }

    class ShardingFindTest extends SqlTestBase {

        private final String name = "ShardingTest_Find";

        public ShardingFindTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            createAndInsertTable();
            testSelect();
        }

        void createAndInsertTable() {
            executeUpdate("drop table IF EXISTS " + name);
            executeUpdate("create table IF NOT EXISTS " + name + "(f1 int primary key, f2 int, f3 int)");
            for (int i = 1; i <= 500; i++) {
                executeUpdate("insert into " + name + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
            }
        }

        void testSelect() {
            sql = "select * from " + name + " where f1 > 490";
            printResultSet();
            sql = "select count(*) from " + name + " where f1 > 490";
            printResultSet();

            sql = "select count(*) from " + name;
            printResultSet();
            sql = "select * from " + name + " where f1 = 3";
            printResultSet();
        }
    }

    class ShardingCrudTest extends SqlTestBase {

        public ShardingCrudTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            testPutRemote();
            insert();
            split();
            select();
            testMultiThread(dbName);
        }

        void testPutRemote() throws Exception {
            String name = "ShardingTest_PutRemote";
            executeUpdate("drop table IF EXISTS " + name);
            executeUpdate("create table IF NOT EXISTS " + name + "(f1 int primary key, f2 int, f3 int)");
            for (int i = 1; i < 500; i += 2) {
                if (i == 67)
                    continue;
                executeUpdate("insert into " + name + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
            }

            for (int i = 2; i < 500; i += 2) {
                executeUpdate("insert into " + name + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
            }
            executeUpdate("insert into " + name + "(f1, f2, f3) values(67,67,67)");
        }

        void insert() throws Exception {
            stmt.executeUpdate("drop table IF EXISTS ShardingTest");
            conn.setAutoCommit(false);
            stmt.executeUpdate("create table IF NOT EXISTS ShardingTest(f1 int SELECTIVITY 10, f2 int, f3 int)");
            conn.commit();
            conn.setAutoCommit(true);
            // stmt.executeUpdate("create index IF NOT EXISTS ShardingTest_i1 on ShardingTest(f1)");

            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(1,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(5,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");

            conn.setAutoCommit(false);
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");
            stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
            conn.commit();
            conn.setAutoCommit(true);
        }

        void select() throws Exception {
            sql = "select distinct * from ShardingTest where f1 > 3";
            sql = "select distinct f1 from ShardingTest";
            printResultSet();
        }

        void testMultiThread(String dbName) throws Exception {
            // 启动两个线程，可以用来测试_rowid_递增的场景
            Thread t1 = new Thread(new MultiThreadShardingCrudTest(dbName));
            Thread t2 = new Thread(new MultiThreadShardingCrudTest(dbName));
            t1.start();
            t2.start();
            t1.join();
            t2.join();
        }

        private class MultiThreadShardingCrudTest extends SqlTestBase implements Runnable {

            public MultiThreadShardingCrudTest(String dbName) {
                super(dbName);
            }

            @Override
            protected void test() throws Exception {
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(3,2,3)");
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(8,2,3)");
            }

            @Override
            public void run() {
                runTest();
            }
        }

        void split() throws Exception {
            for (int i = 0; i < 1000; i++) {
                stmt.executeUpdate("insert into ShardingTest(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
            }
        }
    }
}
