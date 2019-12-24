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
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcDriver;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class JdbcDriverTest extends TestBase {
    @Test
    public void run() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        String url = getURL(LealoneDatabase.NAME);
        JdbcDriver.getConnectionAsync(url, ar -> {
            Connection conn = ar.getResult();
            assertNotNull(conn);
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });
        JdbcDriver.getConnectionAsync("invalid url", ar -> {
            assertTrue(ar.isFailed());
            assertNotNull(ar.getCause());
            latch.countDown();
        });
        latch.await();

        Connection conn = JdbcDriver.getConnection(url);
        assertNotNull(conn);
        conn.close();
    }
}
