/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.sharding;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.DSqlTestBase;

public class ShardingTest extends DSqlTestBase {

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

        // new DdlTest(dbName, true).runTest(); // 在自动提交模式中执行DDL语句，不涉及分布式事务
        new DdlTest(dbName, false).runTest(); // 在手动提交模式中执行DDL语句， 涉及分布式事务

        // new DeleteTest(dbName).runTest();
        // new UpdateTest(dbName).runTest();
        // new SelectTest(dbName).runTest();

        // new TwoTablesTest(dbName).runTest();
        // new ShardingCrudTest(dbName).runTest();
    }

    abstract class CrudTest extends DSqlTestBase {

        final String name = "Sharding" + getClass().getSimpleName();

        public CrudTest(String dbName) {
            super(dbName);
        }

        void createAndInsertTable() {
            executeUpdate("drop table IF EXISTS " + name);
            executeUpdate("create table IF NOT EXISTS " + name + "(f1 int primary key, f2 int, f3 int)");
            for (int i = 1; i <= 50; i++) {
                executeUpdate("insert into " + name + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
            }
        }
    }

    class DdlTest extends CrudTest {

        boolean autoCommit;

        public DdlTest(String dbName, boolean autoCommit) {
            super(dbName);
            this.autoCommit = autoCommit;
        }

        @Override
        protected void test() throws Exception {
            if (!autoCommit) {
                conn.setAutoCommit(false);
            }
            executeUpdate("drop table IF EXISTS " + name);
            // executeUpdate("create table IF NOT EXISTS " + name + "(f1 int primary key, f2 int, f3 int)");
            if (!autoCommit) {
                conn.commit();
                conn.setAutoCommit(true);
            }
        }
    }

    class DeleteTest extends CrudTest {

        public DeleteTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            createAndInsertTable();
            testDelete();
        }

        void testDelete() {
            executeUpdate("delete from " + name + " where f1=10");
            executeUpdate("delete from " + name + " where f1>100");
        }
    }

    class UpdateTest extends CrudTest {

        public UpdateTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            createAndInsertTable();
            testUpdate();
        }

        void testUpdate() {
            executeUpdate("update " + name + " set f2=90 where f1=9");
            executeUpdate("update " + name + " set f2=80 where f1<80");
        }
    }

    class SelectTest extends CrudTest {

        public SelectTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            createAndInsertTable();
            testSelect();
        }

        void testSelect() {
            // sql = "select * from " + name + " where f1 < 10";
            // printResultSet();
            // sql = "select count(*) from " + name + " where f1 < 10";
            // printResultSet();
            //
            // sql = "select count(*) from " + name;
            // printResultSet();
            // sql = "select * from " + name + " where f1 = 3";
            // printResultSet();

            sql = "select * from " + name + " where f1 > 40 order by f1 desc";
            printResultSet();
        }
    }

    class TwoTablesTest extends DSqlTestBase {

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

    class ShardingCrudTest extends DSqlTestBase {

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

        private class MultiThreadShardingCrudTest extends DSqlTestBase implements Runnable {

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
