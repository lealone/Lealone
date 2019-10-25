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
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class JdbcStatementTest extends TestBase {
    @Test
    public void run() throws Exception {
        Connection conn = getConnection(LealoneDatabase.NAME);
        Statement stmt = conn.createStatement();

        stmt.execute("/* test */DROP TABLE IF EXISTS test");
        stmt.execute("CREATE TABLE IF NOT EXISTS test (f1 int, f2 long)");
        stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 2)");

        ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");

        testBatch(stmt);

        stmt.close();
        conn.close();
    }

    void testBatch(Statement stmt) throws SQLException {
        stmt.addBatch("INSERT INTO test(f1, f2) VALUES(1000, 2000)");
        stmt.addBatch("INSERT INTO test(f1, f2) VALUES(8000, 9000)");
        int[] updateCounts = stmt.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
    }
}
