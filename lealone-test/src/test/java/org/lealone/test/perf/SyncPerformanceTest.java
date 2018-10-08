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

import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class SyncPerformanceTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    static Random random = new Random();

    static class MyThread extends Thread {
        Statement stmt;
        Connection conn;
        long read_time;
        long random_read_time;
        long write_time;
        int start;
        int end;

        MyThread(int start, int count) throws Exception {
            super("MyThread-" + start);
            conn = new TestBase().getConnection(LealoneDatabase.NAME);
            stmt = conn.createStatement();
            this.start = start;
            this.end = start + count;
        }

        void write() throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                String sql = "INSERT INTO test(f1, f2) VALUES(" + i + "," + i * 10 + ")";
                stmt.executeUpdate(sql);
            }

            long t2 = System.currentTimeMillis();
            write_time = t2 - t1;
            System.out.println(getName() + " write end, time=" + write_time + " ms");
        }

        void read(boolean random) throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                ResultSet rs;
                if (!random)
                    rs = stmt.executeQuery("SELECT * FROM test where f1 = " + i);
                else
                    rs = stmt.executeQuery("SELECT * FROM test where f1 = " + SyncPerformanceTest.random.nextInt(end));
                while (rs.next()) {
                    // System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            }

            long t2 = System.currentTimeMillis();

            if (random)
                random_read_time = t2 - t1;
            else
                read_time = t2 - t1;
            if (random)
                System.out.println(getName() + " random read end, time=" + random_read_time + " ms");
            else
                System.out.println(getName() + "  read end, time=" + read_time + " ms");
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
    }

    static void run() throws Exception {
        Connection conn = new TestBase().getConnection(LealoneDatabase.NAME);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        stmt.close();

        int threadsCount = 1; // Runtime.getRuntime().availableProcessors() * 4;
        int loop = 1000;

        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new MyThread(i * loop, loop);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            threads[i].join();
        }
        conn.close();

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
        System.out.println("threads: " + threadsCount + ", loop: " + loop + ", rows: " + (threadsCount * loop));
        System.out.println("==========================================================");
        System.out.println("write_sum=" + write_sum + ", avg=" + (write_sum / threadsCount) + " ms");
        System.out.println("read_sum=" + read_sum + ", avg=" + (read_sum / threadsCount) + " ms");
        System.out.println("random_read_sum=" + random_read_sum + ", avg=" + (random_read_sum / threadsCount) + " ms");
    }
}
