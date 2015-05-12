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
package org.lealone.test.sql.misc;

import java.sql.PreparedStatement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class BatchTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testStatementBatch();
        testPreparedStatementBatch();
    }

    void init() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS BatchTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS BatchTest(f1 int, f2 int)");
    }

    void testStatementBatch() throws Exception {
        stmt.clearBatch();
        for (int i = 1; i <= 5; i++) {
            stmt.addBatch("INSERT INTO BatchTest(f1, f2) VALUES(" + i + "," + (i * 2) + ")");
        }

        int[] result = stmt.executeBatch();
        assertEquals(5, result.length);
        for (int i = 1; i <= 5; i++) {
            assertEquals(1, result[i - 1]);
        }

        stmt.clearBatch();
        result = stmt.executeBatch();
        assertEquals(0, result.length);
    }

    void testPreparedStatementBatch() throws Exception {
        sql = "INSERT INTO BatchTest(f1, f2) VALUES(?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= 5; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 2);
            ps.addBatch();
        }

        int[] result = ps.executeBatch();
        assertEquals(5, result.length);
        for (int i = 1; i <= 5; i++) {
            assertEquals(1, result[i - 1]);
        }

        ps.clearBatch();
        result = ps.executeBatch();
        assertEquals(0, result.length);

        ps.close();
    }
}
