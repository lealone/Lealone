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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.lealone.test.sql.TestBase;

public class CRUDExample {
    static Connection getConnection() throws Exception {
        String url = "jdbc:lealone:tcp://localhost:5210/" + TestBase.db //
                + ";default_storage_engine=MVStore;ALIAS_COLUMN_NAME=true";
        //url = "jdbc:lealone:embed:" + TestBase.db + "?default_storage_engine=MVStore";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        return conn;
    }

    public static void main(String[] args) throws Exception {
        crud0();
        //crud();
        //benchmark();
    }

    static void crud0() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
        stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertTrue(rs.next());
        System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
        rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.close();
        conn.close();
    }

    static void crud() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs;

        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long) engine MVStore");
        stmt.executeUpdate("CREATE memory TABLE IF NOT EXISTS test2 (f1 int primary key, f2 long)");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test3 (f1 int primary key, f2 long)");

        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS test_f2 ON test(f2)");

        //stmt.executeUpdate("DELETE FROM test");

        for (int i = 1; i <= 10; i++) {
            stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(" + i + "," + i * 10 + ")");
        }

        stmt.executeUpdate("UPDATE test SET f2 = 1 where f1 = 1");

        //stmt.executeUpdate("UPDATE test SET f2 = 1 where f1 >= 2");

        //rs = stmt.executeQuery("SELECT * FROM test where f1 <= 3");
        rs = stmt.executeQuery("SELECT * FROM test where f1 = 3");
        while (rs.next()) {
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        }
        rs.close();
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");

        rs = stmt.executeQuery("SELECT count(*) FROM test");
        while (rs.next()) {
            System.out.println("count=" + rs.getInt(1));
        }

        rs.close();

        rs = stmt.executeQuery("SELECT count(*) FROM test where f2=20");
        while (rs.next()) {
            System.out.println("count=" + rs.getInt(1));
        }

        rs.close();

        Connection conn2 = getConnection();
        Statement stmt2 = conn2.createStatement();

        rs = stmt2.executeQuery("SELECT count(*) FROM test");
        while (rs.next()) {
            System.out.println("count=" + rs.getInt(1));
        }

        stmt2.close();
        conn2.close();

        stmt.close();
        conn.close();
    }

    static Random random = new Random();
    static CountDownLatch latch;

    static class MyThread extends Thread {
        Statement stmt;
        Connection conn;
        long read_time;
        long randow_read_time;
        long write_time;
        int start;
        int end;

        MyThread(int start, int count) throws Exception {
            super("MyThread-" + start);
            conn = getConnection();
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

        void read(boolean randow) throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                ResultSet rs;
                if (!randow)
                    rs = stmt.executeQuery("SELECT * FROM test where f1 = " + i);
                else
                    rs = stmt.executeQuery("SELECT * FROM test where f1 = " + random.nextInt(end));
                while (rs.next()) {
                    // System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            }

            long t2 = System.currentTimeMillis();

            if (randow)
                randow_read_time = t2 - t1;
            else
                read_time = t2 - t1;
            if (randow)
                System.out.println(getName() + " randow read end, time=" + randow_read_time + " ms");
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
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void benchmark() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        stmt.executeUpdate("set MULTI_THREADED 1");

        //        stmt.close();
        //        conn.close();

        int threadsCount = Runtime.getRuntime().availableProcessors();//10;
        int loop = 10000;
        latch = new CountDownLatch(threadsCount);

        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new MyThread(i * loop, loop);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }

        latch.await();

        long write_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            write_sum += threads[i].write_time;
        }

        long read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            read_sum += threads[i].read_time;
        }
        long randow_read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            randow_read_sum += threads[i].randow_read_time;
        }

        System.out.println();
        System.out.println("threads: " + threadsCount + ", loop: " + loop + ", rows: " + (threadsCount * loop));
        System.out.println("==========================================================");
        System.out.println("write_sum=" + write_sum + ", avg=" + (write_sum / threadsCount) + " ms");
        System.out.println("read_sum=" + read_sum + ", avg=" + (read_sum / threadsCount) + " ms");
        System.out.println("randow_read_sum=" + randow_read_sum + ", avg=" + (randow_read_sum / threadsCount) + " ms");
    }

}
