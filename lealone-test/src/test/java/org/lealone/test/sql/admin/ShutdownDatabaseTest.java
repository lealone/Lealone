/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.admin;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class ShutdownDatabaseTest extends SqlTestBase {

    public ShutdownDatabaseTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest1");
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest2");
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest3");
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest4");

        new ShutdownTest("ShutdownDatabaseTest1", "SHUTDOWN").runTest();
        new ShutdownTest("ShutdownDatabaseTest2", "SHUTDOWN COMPACT").runTest();
        new ShutdownTest("ShutdownDatabaseTest3", "SHUTDOWN DEFRAG").runTest();
        new ShutdownTest("ShutdownDatabaseTest4", "SHUTDOWN IMMEDIATELY").runTest();
    }

    private class ShutdownTest extends SqlTestBase {

        final String name = getClass().getSimpleName();
        final String shutdownStatement;

        public ShutdownTest(String dbName, String shutdownStatement) {
            super(dbName);
            this.shutdownStatement = shutdownStatement;
        }

        void createAndInsertTable() {
            executeUpdate("drop table IF EXISTS " + name);
            executeUpdate("create table IF NOT EXISTS " + name + "(f1 int primary key, f2 int)");
            for (int i = 1; i <= 5; i++) {
                executeUpdate("insert into " + name + "(f1, f2) values(" + i + "," + i + ")");
            }
        }

        @Override
        protected void test() throws Exception {
            createAndInsertTable();
            executeUpdate(shutdownStatement);
        }
    }
}
