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

import org.lealone.client.LealoneClient;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.async.Future;
import org.lealone.test.TestBase;

public class LealoneClientTest {

    public static void main(String[] args) {
        String url = new TestBase().getURL(LealoneDatabase.NAME);
        connectionTest(url);
    }

    static void connectionTest(String url) {
        try {
            Connection conn = LealoneClient.getConnection(url).get();
            for (int i = 0; i < 5; i++) {
                connectionTest(url, i);
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    private static void connectionTest(String url, int loop) throws SQLException {
        int connectionCount = 10000;
        Connection[] connections = new Connection[connectionCount];
        Future[] futures = new Future[connectionCount];
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < connectionCount; i++) {
            int index = i;
            futures[index] = LealoneClient.getConnection(url).onSuccess(c -> {
                connections[index] = c;
            });
        }
        for (int i = 0; i < connectionCount; i++) {
            futures[i].get();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("loop: " + loop + ", connection count: " + connectionCount + ", total time: " + (t2 - t1)
                + " ms" + ", avg time: " + (t2 - t1) / (connectionCount * 1.0) + " ms");
        for (int i = 0; i < connectionCount; i++) {
            connections[i].close();
        }
    }
}
