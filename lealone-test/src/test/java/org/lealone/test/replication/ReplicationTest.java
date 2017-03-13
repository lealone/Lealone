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
package org.lealone.test.replication;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.UnitTestBase;

public class ReplicationTest extends UnitTestBase {
    @Test
    public void run() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationTest (f1 int, f2 long)");

        stmt.executeUpdate("INSERT INTO ReplicationTest(f1, f2) VALUES(1, 2)");
        ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM ReplicationTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));
        rs.close();
        stmt.executeUpdate("DELETE FROM ReplicationTest WHERE f1 = 1");

        stmt.close();
        conn.close();
    }
}
