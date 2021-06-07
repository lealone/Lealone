/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ShardingToShardingTest extends RunModeTest {

    public ShardingToShardingTest() {
        // setHost("127.0.0.2");
    }

    @Test
    public void run() throws Exception {
        alterParameters();
        scaleOut();
        scaleIn();
    }

    private void alterParameters() {
        String dbName = ShardingToShardingTest.class.getSimpleName() + "_alterParameters";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, assignment_factor: 2)");

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE sharding PARAMETERS (QUERY_CACHE_SIZE=20)");
    }

    private void scaleOut() {
        String dbName = ShardingToShardingTest.class.getSimpleName() + "_scaleOut";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding " //
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, assignment_factor: 2)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE sharding PARAMETERS (assignment_factor=3)");
    }

    private void scaleIn() {
        String dbName = ShardingToShardingTest.class.getSimpleName() + "_scaleIn";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding " //
                + "ARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1, assignment_factor: 3)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE sharding PARAMETERS (assignment_factor=2)");
    }
}
