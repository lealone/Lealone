/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ShardingToReplicationTest extends RunModeTest {

    public ShardingToReplicationTest() {
        // setHost("127.0.0.2");
    }

    @Test
    public void run() throws Exception {
        String dbName = ShardingToShardingTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, assignment_factor: 2)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE replication " // //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 3)");
    }
}
