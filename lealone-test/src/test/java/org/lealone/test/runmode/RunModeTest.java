/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.DSqlTestBase;

public abstract class RunModeTest extends DSqlTestBase {

    public RunModeTest() {
        super(LealoneDatabase.NAME); // 连到LealoneDatabase才能执行CREATE DATABASE
    }

    protected void crudTest(String dbName) {
        class CrudTest2 extends DSqlTestBase {
            public CrudTest2(String dbName) {
                super(dbName);
            }

            @Override
            protected void test() throws Exception {
                String tableName = "run_mode_test";
                executeUpdate("drop table IF EXISTS " + tableName);
                executeUpdate("create table IF NOT EXISTS " + tableName + "(f1 int primary key, f2 int, f3 int)");

                for (int i = 1; i <= 300; i++) {
                    executeUpdate("insert into " + tableName + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
                }
            }
        }
        new CrudTest2(dbName).runTest();
    }

    protected static class CrudTest extends DSqlTestBase {

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
            for (int j = 0; j < 50; j++) {
                // long t1 = System.currentTimeMillis();
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
                // long t2 = System.currentTimeMillis();
                // System.out.println(t2 - t1);
            }

            // StringBuilder sql = new StringBuilder();
            // int rows = 200;
            // for (int j = 0; j < rows; j++) {
            // sql.append("insert into test values(0,1,2);");
            // }
            // sql.setLength(sql.length() - 1);
            // executeUpdate(sql.toString()); //TODO 异步化后导致不能批理更新了
        }

        void select() {
            sql = "select distinct * from test where f1 > 3";
            sql = "select distinct f1 from test";
            printResultSet();
        }

        void batch() {
            int count = 0;
            for (int i = 0; i < count; i++) {
                String tableName = "run_mode_test_" + i;
                executeUpdate("create table IF NOT EXISTS " + tableName + "(f0 int, f1 int, f2 int, f3 int, f4 int,"
                        + " f5 int, f6 int, f7 int, f8 int, f9 int)");
            }
        }
    }
}
