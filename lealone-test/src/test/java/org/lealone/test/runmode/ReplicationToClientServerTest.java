/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ReplicationToClientServerTest extends RunModeTest {

    public ReplicationToClientServerTest() {
        setHost("127.0.0.1");
    }

    @Test
    public void run() throws Exception {
        String dbName = ReplicationToClientServerTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE replication "
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        crudTest(dbName);

        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE client_server");
    }
}
