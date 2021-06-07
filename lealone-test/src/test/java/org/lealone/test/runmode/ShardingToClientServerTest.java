/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;
import org.lealone.test.sql.DSqlTestBase;

public class ShardingToClientServerTest extends RunModeTest {

    String tableName = "talbe_" + ShardingToClientServerTest.class.getSimpleName();;

    public ShardingToClientServerTest() {
        // setHost("127.0.0.2");
    }

    @Test
    public void run() throws Exception {
        String dbName = ShardingToClientServerTest.class.getSimpleName();
        createTest(dbName);
        insertTest(dbName);

        // executeUpdate("ALTER DATABASE " + dbName + " RUN MODE client_server");
    }

    void createTest(String dbName) {
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2, assignment_factor: 5)");

        class CreateTest extends DSqlTestBase {
            public CreateTest(String dbName) {
                super(dbName);
            }

            @Override
            protected void test() throws Exception {
                executeUpdate("drop table IF EXISTS " + tableName);
                executeUpdate("create table IF NOT EXISTS " + tableName + "(f1 int primary key, f2 int, f3 int)");
            }
        }
        new CreateTest(dbName).runTest();
    }

    void insertTest(String dbName) {
        class InsertTest extends DSqlTestBase {
            public InsertTest(String dbName) {
                super(dbName);
            }

            @Override
            protected void test() throws Exception {
                for (int i = 901; i <= 1200; i++) {
                    executeUpdate("insert into " + tableName + "(f1, f2, f3) values(" + i + "," + i + "," + i + ")");
                }
            }
        }
        new InsertTest(dbName).runTest();
    }
}
