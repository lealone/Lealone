/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ClientServerToShardingTest extends RunModeTest {

    public ClientServerToShardingTest() {
        setHost("127.0.0.1");
    }

    @Test
    public void run() throws Exception {
        String dbName = ClientServerToShardingTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE client_server");

        new CrudTest(dbName).runTest();

        sql = "ALTER DATABASE " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2, assignment_factor :3)";
        executeUpdate(sql);
    }
}
