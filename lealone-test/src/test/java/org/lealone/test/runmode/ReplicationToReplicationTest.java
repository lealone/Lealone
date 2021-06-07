/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ReplicationToReplicationTest extends RunModeTest {

    public ReplicationToReplicationTest() {
        // setHost("127.0.0.2");
    }

    @Test
    public void run() throws Exception {
        alterParameters();
        scaleOut();
        scaleIn();
    }

    private void alterParameters() {
        String dbName = ReplicationToReplicationTest.class.getSimpleName() + "_alterParameters";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName
                + "  RUN MODE replication PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        executeUpdate("ALTER DATABASE " + dbName + " PARAMETERS (QUERY_CACHE_SIZE=20)");
    }

    private void scaleOut() {
        String dbName = ReplicationToReplicationTest.class.getSimpleName() + "_scaleOut";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName
                + "  RUN MODE replication PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName //
                + " RUN MODE replication PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 3)");
    }

    private void scaleIn() {
        String dbName = ReplicationToReplicationTest.class.getSimpleName() + "_scaleIn";
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName
                + "  RUN MODE replication PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 3)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName //
                + " RUN MODE replication PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");
    }
}
