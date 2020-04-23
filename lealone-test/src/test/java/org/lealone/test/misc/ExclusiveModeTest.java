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
package org.lealone.test.misc;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExclusiveModeTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        final Connection conn1 = getConnection();
        final Connection conn2 = getConnection();

        // Thread-1使用了排它模式，只有它运行完时，才到Thread-2
        Thread t1 = new Thread(() -> {
            try {
                Statement stmt = conn1.createStatement();
                stmt.executeUpdate("set EXCLUSIVE 1");
                Thread.sleep(2000);
                stmt.close();
                conn1.close();
                System.out.println("Thread-1 end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(1000);
                Statement stmt = conn2.createStatement();
                stmt.executeQuery("select 1");
                stmt.close();
                conn2.close();
                System.out.println("Thread-2 end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}
