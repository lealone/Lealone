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
import java.sql.ResultSet;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class AsyncConcurrentUpdateTest {

    public static void main(String[] args) throws Exception {
        Connection conn = new TestBase().getConnection(LealoneDatabase.NAME);
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS AsyncConcurrentUpdateTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS AsyncConcurrentUpdateTest (f1 int primary key, f2 long)");
        String sql = "INSERT INTO AsyncConcurrentUpdateTest(f1, f2) VALUES(1, 2)";
        stmt.executeUpdate(sql);

        int threadsCount = 2;
        UpdateThread[] updateThreads = new UpdateThread[threadsCount];
        DeleteThread[] deleteThreads = new DeleteThread[threadsCount];
        QueryThread[] queryThreads = new QueryThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i] = new UpdateThread(i);
        }
        for (int i = 0; i < threadsCount; i++) {
            deleteThreads[i] = new DeleteThread(i);
        }
        for (int i = 0; i < threadsCount; i++) {
            queryThreads[i] = new QueryThread(i);
        }

        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i].start();
            deleteThreads[i].start();
            queryThreads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i].join();
            deleteThreads[i].join();
            queryThreads[i].join();
        }
        JdbcStatementTest.close(stmt, conn);
    }

    static class UpdateThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        UpdateThread(int id) throws Exception {
            super("UpdateThread-" + id);
            conn = new TestBase().getConnection(LealoneDatabase.NAME);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);

                String sql = "update AsyncConcurrentUpdateTest set f2=3 where f1=1";
                stmt.executeUpdate(sql);

                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                JdbcStatementTest.close(stmt, conn);
            }
        }
    }

    static class DeleteThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        DeleteThread(int id) throws Exception {
            super("DeleteThread-" + id);
            conn = new TestBase().getConnection(LealoneDatabase.NAME);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);

                String sql = "delete from AsyncConcurrentUpdateTest where f1=1";
                stmt.executeUpdate(sql);

                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                JdbcStatementTest.close(stmt, conn);
            }
        }
    }

    static class QueryThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        QueryThread(int id) throws Exception {
            super("QueryThread-" + id);
            conn = new TestBase().getConnection(LealoneDatabase.NAME);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                ResultSet rs = stmt.executeQuery("SELECT * FROM AsyncConcurrentUpdateTest where f1 = 1");
                while (rs.next()) {
                    System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                JdbcStatementTest.close(stmt, conn);
            }
        }
    }
}
