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
package org.lealone.test.replication;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class ReplicationTest extends SqlTestBase {

    private static final String REPLICATION_DB_NAME = "ReplicationTestDB4";

    public ReplicationTest() {
        // super(REPLICATION_DB_NAME, RunMode.REPLICATION); // 自动创建一个使用复制模式的ReplicationTestDB数据库
        // setHost("127.0.0.1");
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        sql = "CREATE DATABASE IF NOT EXISTS " + REPLICATION_DB_NAME
                + " RUN MODE replication  PARAMETERS (replication_factor: 2)";
        stmt.executeUpdate(sql);

        // new SyncReplicationTest().runTest();
        new AsyncReplicationTest().runTest();
    }

    class AsyncReplicationTest extends SqlTestBase {

        public AsyncReplicationTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            executeUpdate("drop table IF EXISTS AsyncReplicationTest");
            executeUpdate("create table IF NOT EXISTS AsyncReplicationTest(f1 int primary key, f2 int)");

            CountDownLatch latch = new CountDownLatch(2);
            JdbcStatement stmt = (JdbcStatement) this.stmt;
            stmt.executeUpdateAsync("insert into AsyncReplicationTest(f1, f2) values(10, 20)", ar -> {
                latch.countDown();
            });

            stmt.executeQueryAsync("select * from AsyncReplicationTest", ar -> {
                if (ar.isSucceeded()) {
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        System.out.println(rs.getString(1));
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            });
            latch.await();
        }
    }

    class SyncReplicationTest extends SqlTestBase {

        public SyncReplicationTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationTest");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationTest (f1 int primary key, f2 long)");
            // stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationTest (f1 int, f2 long)");
            // stmt.executeUpdate("INSERT INTO ReplicationTest(f1, f2) VALUES(1, 2)");

            // 启动两个新事务更新同一行，可以用来测试Replication冲突的场景
            Thread t1 = new Thread(new CrudTest("INSERT INTO ReplicationTest(f1, f2) VALUES(3, 4)"));
            Thread t2 = new Thread(new CrudTest("INSERT INTO ReplicationTest(f1, f2) VALUES(5, 6)"));
            t1.start();
            t2.start();
            t1.join();
            t2.join();

            ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM ReplicationTest");
            // assertTrue(rs.next());
            // assertEquals(1, rs.getInt(1));
            // assertEquals(20, rs.getLong(2));
            rs.close();
            // stmt.executeUpdate("DELETE FROM ReplicationTest WHERE f1 = 1");
        }
    }

    private static class CrudTest extends SqlTestBase implements Runnable {
        String insert;

        public CrudTest(String insert) {
            super(REPLICATION_DB_NAME);
            this.insert = insert;
        }

        @Override
        protected void test() throws Exception {
            stmt.executeUpdate(insert);
            // conn.setAutoCommit(false);
            // stmt.executeUpdate("update ReplicationTest set f2 = 20 where f1 = 1");
            // conn.commit();

            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
        }

        @Override
        public void run() {
            runTest();
        }
    }
}
