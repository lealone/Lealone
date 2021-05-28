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
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class JdbcConnectionTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        testTransactionIsolationLevel();

        int count = 1;
        count = 1;
        for (int i = 0; i < count; i++) {
            long t1 = System.currentTimeMillis();
            run2();
            long t2 = System.currentTimeMillis();
            System.out.println("loop: " + (i + 1) + ", time: " + (t2 - t1) + " ms");
        }
    }

    void testTransactionIsolationLevel() throws Exception {
        int til = conn.getTransactionIsolation();
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, til); // 默认是读已提交级别
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        conn.setTransactionIsolation(til);

        try {
            conn.setTransactionIsolation(-1);
            fail();
        } catch (SQLException e) {
        }
    }

    public void run2() throws Exception {
        // addConnectionParameter(ConnectionSetting.TRACE_ENABLED.name(), true);
        // addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", TraceSystem.DEBUG);

        Connection conn0 = getConnection(LealoneDatabase.NAME);
        JdbcStatement stmt0 = (JdbcStatement) conn0.createStatement();
        stmt0.execute("CREATE TABLE IF NOT EXISTS JdbcConnectionTest (f1 int, f2 long)");

        stmt0.executeUpdate("DROP TABLE IF EXISTS test");
        stmt0.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        stmt0.executeUpdate("INSERT INTO test(f1, f2) VALUES(3, 2)");
        stmt0.execute("SELECT * FROM test");
        ResultSet rs = stmt0.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        conn0.close();

        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                Connection conn;
                try {
                    conn = getConnection(LealoneDatabase.NAME);
                    JdbcStatement stmt = (JdbcStatement) conn.createStatement();
                    // stmt.executeQuery("SELECT * FROM SYS");
                    String sql = "show tables";

                    sql = "insert into JdbcConnectionTest(f1,f2) values(1,2)";
                    int asyncCount = 20;
                    CountDownLatch asyncLatch = new CountDownLatch(asyncCount);
                    for (int j = 0; j < asyncCount; j++) {

                        stmt.executeUpdateAsync(sql).onComplete(ar -> {
                            asyncLatch.countDown();
                        });

                        // stmt.executeQueryAsync(sql, ar -> {
                        // try {
                        // if (ar.isSucceeded()) {
                        // ResultSet rs = ar.getResult();
                        // try {
                        // while (rs.next()) {
                        // System.out.println(rs.getString(1));
                        // }
                        // } catch (SQLException e) {
                        // e.printStackTrace();
                        // }
                        // }
                        // } finally {
                        // asyncLatch.countDown();
                        // }
                        // });
                    }
                    asyncLatch.await();
                    // stmt.execute("CREATE TABLE IF NOT EXISTS JdbcConnectionTest (f1 int, f2 long)");

                    // ResultSet rs = stmt.executeQuery("SELECT * FROM SYS");
                    // assertTrue(rs.next());
                    // assertEquals(1, rs.getInt(1));
                    // assertEquals(2, rs.getLong(2));

                    // stmt.executeUpdate("DELETE FROM JdbcConnectionTest WHERE f1 = 1");

                    stmt.close();
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
    }
}
