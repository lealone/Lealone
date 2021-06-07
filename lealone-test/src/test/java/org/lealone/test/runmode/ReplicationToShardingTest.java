/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ReplicationToShardingTest extends RunModeTest {

    public ReplicationToShardingTest() {
        setHost("127.0.0.1");
    }

    @Test
    public void run() throws Exception {
        String dbName = ReplicationToShardingTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE replication " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        new CrudTest(dbName).runTest();

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, assignment_factor: 2)");
    }
}
