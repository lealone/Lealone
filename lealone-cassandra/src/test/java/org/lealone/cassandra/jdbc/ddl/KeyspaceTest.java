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
package org.lealone.cassandra.jdbc.ddl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.lealone.cassandra.jdbc.TestBase;

public class KeyspaceTest extends TestBase {
    @Test
    public void run() throws Exception {
        create();
        alter();
        drop();
    }

    void create() throws Exception {
        sql = "CREATE KEYSPACE IF NOT EXISTS KeyspaceTest " + //
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true";
        executeUpdate();
    }

    void alter() throws Exception {
        sql = "ALTER KEYSPACE KeyspaceTest " + //
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':3} AND DURABLE_WRITES = true";
        executeUpdate();
    }

    void drop() throws Exception {
        sql = "DROP KEYSPACE IF EXISTS KeyspaceTest";
        executeUpdate();
        executeUpdate();
        sql = "DROP KEYSPACE KeyspaceTest";
        assertEquals(tryExecuteUpdate(), -1);
    }
}
