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
package org.lealone.test.perf;

import java.sql.Connection;
import java.sql.DriverManager;

import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class ConnectionPerfTest {

    public static void main(String[] args) throws Exception {
        String url = new TestBase().getURL(LealoneDatabase.NAME);
        Connection conn = getConnection(url);
        int loop = 1;
        for (int i = 0; i < loop; i++) {
            run(url);
        }
        conn.close();
    }

    static Connection getConnection(String url) throws Exception {
        return DriverManager.getConnection(url);
    }

    static void run(String url) throws Exception {
        int count = 2000;
        Connection[] connections = new Connection[count];

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            connections[i] = getConnection(url);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("connection count: " + count + ", total time: " + (t2 - t1) + " ms" + ", avg time: "
                + (t2 - t1) / (count * 1.0) + " ms");

        for (int i = 0; i < count; i++) {
            connections[i].close();
        }
    }
}
