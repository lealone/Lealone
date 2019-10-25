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
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcPreparedStatement;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class JdbcPreparedStatementTest extends TestBase {
    @Test
    public void run() throws Exception {
        Connection conn = getConnection(LealoneDatabase.NAME);
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");

        String sql = "INSERT INTO test(f1, f2) VALUES(?, ?)";
        JdbcPreparedStatement ps = (JdbcPreparedStatement) conn.prepareStatement(sql);
        ResultSetMetaData md = ps.getMetaData();
        assertNull(md);
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertNotNull(pmd);
        assertEquals(2, pmd.getParameterCount());

        ps.setInt(1, 10);
        ps.setLong(2, 20);
        ps.executeUpdate();

        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            ps.setInt(1, i * 100);
            ps.setLong(2, 2 * i * 200);
            ps.executeUpdateAsync(ar -> {
                latch.countDown();
            });
        }
        latch.await();

        ps = (JdbcPreparedStatement) conn.prepareStatement("SELECT * FROM test where f2 > ?");
        md = ps.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());

        CountDownLatch latch2 = new CountDownLatch(1);
        ps.setLong(1, 2);
        ps.executeQueryAsync(ar -> {
            ResultSet rs = ar.getResult();
            try {
                while (rs.next()) {
                    System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            latch2.countDown();
        });
        ps.executeQueryAsync(null);
        latch2.await();
        conn.close();
    }
}
