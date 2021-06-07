/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;
import org.lealone.test.sql.SqlTestBase;

public class AsyncConcurrentUpdateTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
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
        close(stmt, conn);
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
                close(stmt, conn);
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
                close(stmt, conn);
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
                close(stmt, conn);
            }
        }
    }

    static void close(JdbcStatement stmt, Connection conn) {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
