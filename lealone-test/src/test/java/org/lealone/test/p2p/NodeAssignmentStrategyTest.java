/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.p2p;

import org.junit.Test;
import org.lealone.test.runmode.RunModeTest;

public class NodeAssignmentStrategyTest extends RunModeTest {

    public NodeAssignmentStrategyTest() {
    }

    @Test
    public void run() throws Exception {
        String dbName = NodeAssignmentStrategyTest.class.getSimpleName() + "_Random";
        sql = "CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding";
        sql += " PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1,";
        sql += " node_assignment_strategy: 'RandomNodeAssignmentStrategy', assignment_factor: 3)";
        executeUpdate(sql);

        dbName = NodeAssignmentStrategyTest.class.getSimpleName() + "_LoadBased";
        sql = "CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding";
        sql += " PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1,";
        sql += " node_assignment_strategy: 'LoadBasedNodeAssignmentStrategy', assignment_factor: 2)";
        executeUpdate(sql);

        dbName = NodeAssignmentStrategyTest.class.getSimpleName() + "_Manual";
        sql = "CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE sharding";
        sql += " PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 1,";
        sql += " node_assignment_strategy: 'ManualNodeAssignmentStrategy', assignment_factor: 2, "
                + "host_id_list: '127.0.0.1:9210,127.0.0.3:9210')";
        executeUpdate(sql);
    }
}
