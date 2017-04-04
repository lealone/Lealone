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
package org.lealone.test.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionTest {

    static Connection getConnection(String url) throws Exception {
        return DriverManager.getConnection(url);
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:h2:tcp://localhost:9092/test;user=sa;password=";
        url = "jdbc:lealone:tcp://localhost:7210/test;user=sa;password=";

        Connection conn = getConnection(url);
        int loop = 1;
        for (int i = 0; i < loop; i++) {
            run(url);
        }
        conn.close();
    }

    static void run(String url) throws Exception {
        int count = 10000;
        Connection[] connections = new Connection[count];

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            connections[i] = getConnection(url);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("time=" + (t2 - t1) + " ms");
        // Thread.sleep(1000);
        for (int i = 0; i < count; i++) {
            connections[i].close();
        }
    }

}
