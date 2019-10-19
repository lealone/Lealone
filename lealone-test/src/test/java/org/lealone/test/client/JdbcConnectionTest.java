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
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.common.trace.TraceSystem;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class JdbcConnectionTest extends TestBase {

    @Test
    public void run() throws Exception {
        addConnectionParameter("TRACE_ENABLED", true);
        addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", TraceSystem.DEBUG);
        int count = 1;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                Connection conn;
                try {
                    conn = getConnection(LealoneDatabase.NAME);
                    Statement stmt = conn.createStatement();
                    // stmt.executeQuery("SELECT * FROM SYS");
                    stmt.executeQuery("show tables");
                    // stmt.execute("CREATE TABLE IF NOT EXISTS JdbcConnectionTest (f1 int, f2 long)");

                    // ResultSet rs = stmt.executeQuery("SELECT * FROM SYS");
                    // assertTrue(rs.next());
                    // assertEquals(1, rs.getInt(1));
                    // assertEquals(2, rs.getLong(2));

                    // stmt.executeUpdate("DELETE FROM JdbcConnectionTest WHERE f1 = 1");

                    stmt.close();
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
    }
}
