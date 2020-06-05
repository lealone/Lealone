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
package org.lealone.test.sql.replication;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class ReplicationTest extends SqlTestBase {

    private static final String REPLICATION_DB_NAME = "ReplicationTestDB";

    public ReplicationTest() {
        // super(REPLICATION_DB_NAME, RunMode.REPLICATION); // 自动创建一个使用复制模式的ReplicationTestDB数据库
        // setHost("127.0.0.1");
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        sql = "CREATE DATABASE IF NOT EXISTS " + REPLICATION_DB_NAME
                + " RUN MODE replication PARAMETERS (replication_factor: 3)";
        stmt.executeUpdate(sql);

        // new AsyncReplicationTest().runTest();
        // new ReplicationConflictTest().runTest();
        // new ReplicationAppendTest().runTest();
        new ReplicationDdlConflictTest().runTest();
        // new ReplicationUpdateRowLockConflictTest().runTest();
        // new ReplicationDeleteRowLockConflictTest().runTest();
    }

    static class AsyncReplicationTest extends SqlTestBase {

        public AsyncReplicationTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            executeUpdate("drop table IF EXISTS AsyncReplicationTest");
            executeUpdate("create table IF NOT EXISTS AsyncReplicationTest(f1 int primary key, f2 int)");

            JdbcStatement stmt = (JdbcStatement) this.stmt;
            stmt.executeUpdateAsync("insert into AsyncReplicationTest(f1, f2) values(10, 20)").get();

            stmt.executeQueryAsync("select * from AsyncReplicationTest").onSuccess(rs -> {
                try {
                    rs.next();
                    System.out.println(rs.getString(1));
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }).onFailure(t -> {
                t.printStackTrace();
            }).get();
        }
    }

    static class ReplicationConflictTest extends SqlTestBase {

        public ReplicationConflictTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationTest");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationTest (f1 int primary key, f2 long)");

            // 启动两个新事务更新同一行，可以用来测试Replication冲突的场景
            Thread t1 = new Thread(new InsertTest());
            Thread t2 = new Thread(new UpdateTest(200));
            Thread t3 = new Thread(new UpdateTest(300));
            Thread t4 = new Thread(new UpdateTest(400));
            t1.start();
            t2.start();
            t3.start();
            t4.start();
            t1.join();
            t2.join();
            t3.join();
            t4.join();

            ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM ReplicationTest");
            // assertTrue(rs.next());
            // assertEquals(1, rs.getInt(1));
            // assertEquals(20, rs.getLong(2));
            rs.close();
            // stmt.executeUpdate("DELETE FROM ReplicationTest WHERE f1 = 1");
        }
    }

    static class ReplicationAppendTest extends SqlTestBase {

        static class InsertTest extends CrudTest {
            int value;

            public InsertTest(int v) {
                value = v;
            }

            @Override
            protected void test() throws Exception {
                int f1 = value * 10;
                int f2 = value * 100;
                String sql = "INSERT INTO ReplicationAppendTest(f1, f2) VALUES(" + f1 + ", " + f2 + ")";
                stmt.executeUpdate(sql);
            }
        }

        public ReplicationAppendTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationAppendTest");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationAppendTest (f1 int, f2 long)");

            Thread t1 = new Thread(new InsertTest(1));
            Thread t2 = new Thread(new InsertTest(2));
            Thread t3 = new Thread(new InsertTest(3));
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();
        }
    }

    static class ReplicationDdlConflictTest extends SqlTestBase {

        static class DdlTest extends CrudTest {
            @Override
            protected void test() throws Exception {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS DdlTest (f1 int, f2 long)");
            }
        }

        public ReplicationDdlConflictTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            Thread t1 = new Thread(new DdlTest());
            Thread t2 = new Thread(new DdlTest());
            Thread t3 = new Thread(new DdlTest());
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();
        }
    }

    static class ReplicationUpdateRowLockConflictTest extends SqlTestBase {
        static int key = 1;

        static class UpdateTest extends CrudTest {
            int value;

            public UpdateTest(int v) {
                value = v;
            }

            @Override
            protected void test() throws Exception {
                String sql = "update ReplicationUpdateRowLockConflictTest set f2 = " + value + " where f1 = " + key;
                stmt.executeUpdate(sql);
            }
        }

        public ReplicationUpdateRowLockConflictTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            init();
            Thread t1 = new Thread(new UpdateTest(1));
            Thread t2 = new Thread(new UpdateTest(2));
            Thread t3 = new Thread(new UpdateTest(3));
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();
        }

        private void init() throws Exception {
            stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationUpdateRowLockConflictTest");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationUpdateRowLockConflictTest (f1 int, f2 long)");
            int f2 = key * 100;
            String sql = "INSERT INTO ReplicationUpdateRowLockConflictTest(f1, f2) VALUES(" + key + ", " + f2 + ")";
            stmt.executeUpdate(sql);
        }
    }

    static class ReplicationDeleteRowLockConflictTest extends SqlTestBase {

        static class DeleteTest extends CrudTest {
            @Override
            protected void test() throws Exception {
                stmt.executeUpdate("delete from ReplicationDeleteRowLockConflictTest where f1 = 1");
            }
        }

        public ReplicationDeleteRowLockConflictTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            init();
            Thread t1 = new Thread(new DeleteTest());
            Thread t2 = new Thread(new DeleteTest());
            Thread t3 = new Thread(new DeleteTest());
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();
        }

        private void init() throws Exception {
            stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationDeleteRowLockConflictTest");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationDeleteRowLockConflictTest (f1 int, f2 long)");
            String sql = "INSERT INTO ReplicationDeleteRowLockConflictTest(f1, f2) VALUES(1, 2)";
            stmt.executeUpdate(sql);
        }
    }

    static class InsertTest extends CrudTest {
        @Override
        protected void test() throws Exception {
            stmt.executeUpdate("INSERT INTO ReplicationTest(f1, f2) VALUES(1, 2)");
        }
    }

    static class QueryTest extends CrudTest {
        @Override
        protected void test() throws Exception {
            sql = "select * from ReplicationTest where f1 = 1";
            printResultSet();
        }
    }

    static class UpdateTest extends CrudTest {
        int value;

        public UpdateTest(int v) {
            value = v;
        }

        @Override
        protected void test() throws Exception {
            // conn.setAutoCommit(false);
            stmt.executeUpdate("update ReplicationTest set f2 = " + value + " where f1 = 1");
            // conn.commit();
        }
    }

    static class SelectTest extends CrudTest {
        @Override
        protected void test() throws Exception {
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
            sql = "select * from ReplicationTest where _rowid_=1";
            printResultSet();
        }
    }

    static abstract class CrudTest extends SqlTestBase implements Runnable {
        public CrudTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        public void run() {
            runTest();
        }
    }
}
