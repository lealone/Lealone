/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ReplicationToReplicationTest extends RunModeTest {

    public ReplicationToReplicationTest() {
        // setHost("127.0.0.2");
    }

    @Test
    @Override
    public void run() throws Exception {
        scaleOut();
        scaleIn();
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
