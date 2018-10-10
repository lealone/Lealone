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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.test.TestBase;

public class AsyncPerformanceTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    static void run() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        int loop = 20;
        for (int i = 0; i < loop; i++) {
            stmt.executeUpdate("DROP TABLE IF EXISTS test");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
            benchmark(i + 1);
        }

        stmt.close();
        conn.close();
    }

    static Connection getConnection() throws Exception {
        long t1 = System.currentTimeMillis();
        TestBase t = new TestBase();
        // t.setNetFactoryName("nio");
        Connection conn = t.getConnection(LealoneDatabase.NAME);
        long t2 = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName() + " getConnection time: " + (t2 - t1) + " ms");
        return conn;
    }

    static void benchmark(int loop) throws Exception {
        int threadsCount = 1; // Runtime.getRuntime().availableProcessors() * 4;
        int count = 500;

        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new MyThread(i * count, count);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            threads[i].join();
        }

        long write_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            write_sum += threads[i].write_time;
        }

        long read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            read_sum += threads[i].read_time;
        }
        long random_read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            random_read_sum += threads[i].random_read_time;
        }

        System.out.println();
        System.out.println("threads: " + threadsCount + ", loop: " + loop + ", rows: " + (threadsCount * count));
        System.out.println("==========================================================");
        System.out.println("write_sum=" + write_sum + " ms, avg=" + (write_sum / threadsCount) + " ms");
        System.out.println("read_sum=" + read_sum + " ms, avg=" + (read_sum / threadsCount) + " ms");
        System.out
                .println("random_read_sum=" + random_read_sum + " ms, avg=" + (random_read_sum / threadsCount) + " ms");
    }

    static Random random = new Random();

    static class MyThread extends Thread {
        JdbcStatement stmt;
        Connection conn;
        long read_time;
        long random_read_time;
        long write_time;
        int start;
        int end;

        MyThread(int start, int count) throws Exception {
            super("MyThread-" + start);
            conn = getConnection();
            stmt = (JdbcStatement) conn.createStatement();
            this.start = start;
            this.end = start + count;
        }

        @Override
        public void run() {
            try {
                write();
                read(false);
                read(true);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void write() throws Exception {
            CountDownLatch latch = new CountDownLatch(end - start);
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                // System.out.println("i: " + i);
                String sql = "INSERT INTO test(f1, f2) VALUES(" + i + "," + i * 10 + ")";
                stmt.executeUpdateAsync(sql, res -> {
                    latch.countDown();
                    // long count = latch.getCount();
                    // System.out.println((end - start) + " " + count);
                });
            }
            latch.await();

            long t2 = System.currentTimeMillis();
            write_time = t2 - t1;
            // System.out.println(getName() + " write end, time=" + write_time + " ms");
        }

        void read(boolean random) throws Exception {
            CountDownLatch latch = new CountDownLatch(end - start);
            long t1 = System.currentTimeMillis();

            AsyncHandler<AsyncResult<ResultSet>> handler = ac -> {
                ResultSet rs = ac.getResult();
                try {
                    while (rs.next()) {
                        // System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                    }
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            for (int i = start; i < end; i++) {
                if (!random)
                    stmt.executeQueryAsync("SELECT * FROM test where f1 = " + i, handler);
                else
                    stmt.executeQueryAsync("SELECT * FROM test where f1 = " + AsyncPerformanceTest.random.nextInt(end),
                            handler);
            }
            latch.await();
            long t2 = System.currentTimeMillis();

            if (random)
                random_read_time = t2 - t1;
            else
                read_time = t2 - t1;
            // if (random)
            // System.out.println(getName() + " random read end, time=" + random_read_time + " ms");
            // else
            // System.out.println(getName() + " read end, time=" + read_time + " ms");
        }
    }
}
