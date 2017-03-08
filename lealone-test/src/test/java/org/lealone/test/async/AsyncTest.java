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
package org.lealone.test.async;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.lealone.test.sql.SqlTestBase;

public class AsyncTest extends SqlTestBase {

    public static void main(String[] args) throws Exception {
        new AsyncTest().run();
    }

    class MyThread extends Thread {
        Connection connection;
        Statement statement;

        public MyThread() {
            try {
                connection = AsyncTest.this.getConnection();
                statement = connection.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Random random = new Random();

        @Override
        public void run() {
            try {
                // insert();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            int loop = 10;
            while (loop-- > 0) {
                try {
                    stmt.executeQuery("SELECT * FROM AsyncTest where pk = " + random.nextInt(20000));
                    // statement.executeQuery(AsyncTest.this.sql);
                    // Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void close() throws SQLException {
            statement.close();
            connection.close();
        }

        void insert() throws Exception {
            int loop = 20000;
            for (int i = 10000; i < loop; i++)
                statement.executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('" + i + "', 'a1', 'b', 51)");
        }

    }

    public void run() throws Exception {
        // init();
        sql = "select * from AsyncTest where pk = '01'";
        // oneThread();
        multiThreads();
    }

    public void multiThreads() throws Exception {
        int count = 1;
        MyThread[] threads = new MyThread[count];

        for (int i = 0; i < count; i++) {
            threads[i] = new MyThread();
        }

        for (int i = 0; i < count; i++) {
            threads[i].start();
        }

        for (int i = 0; i < count; i++) {
            threads[i].join();
        }

        for (int i = 0; i < count; i++) {
            threads[i].close();
        }
    }

    public void oneThread() throws Exception {
        int count = 342;
        Connection[] connections = new Connection[count];

        Statement[] statements = new Statement[count];

        for (int i = 0; i < count; i++) {
            connections[i] = getConnection();
            statements[i] = connections[i].createStatement();
        }

        int loop = 1000000;
        while (loop-- > 0) {
            for (int i = 0; i < count; i++) {
                statements[i].executeQuery(sql);
            }
            // Thread.sleep(1000);

            Thread.sleep(500);
        }

        for (int i = 0; i < count; i++) {
            statements[i].close();
            connections[i].close();
        }
    }

    void init() throws Exception {
        createTable("AsyncTest");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO AsyncTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");
    }
}
