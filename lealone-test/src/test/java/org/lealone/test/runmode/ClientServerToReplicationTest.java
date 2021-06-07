/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ClientServerToReplicationTest extends RunModeTest {

    public ClientServerToReplicationTest() {
        setHost("127.0.0.1");
    }

    @Test
    public void run() throws Exception {
        String dbName = ClientServerToReplicationTest.class.getSimpleName();
        sql = "CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE client_server";
        sql += " PARAMETERS (node_assignment_strategy: 'RandomNodeAssignmentStrategy')";
        executeUpdate(sql);

        Thread t = new Thread(() -> {
            new CrudTest(dbName).runTest();
        });
        t.start();
        t.join();

        sql = "ALTER DATABASE " + dbName + " RUN MODE replication";
        sql += " PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2,";
        sql += " node_assignment_strategy: 'RandomNodeAssignmentStrategy')";
        executeUpdate(sql);
    }
}
