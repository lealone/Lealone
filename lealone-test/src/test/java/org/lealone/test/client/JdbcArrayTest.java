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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcArray;
import org.lealone.test.sql.SqlTestBase;

public class JdbcArrayTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcArrayTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcArrayTest (f1 int, f2 long, f3 array)");

        JdbcArray array = (JdbcArray) conn.createArrayOf(null, new Object[] { 1, 2, 3 });

        PreparedStatement ps = conn.prepareStatement("INSERT INTO JdbcArrayTest(f1, f2, f3) VALUES(1, 2, ?)");
        ps.setArray(1, array);
        ps.executeUpdate();

        ResultSet rs = stmt.executeQuery("SELECT f1, f2, f3 FROM JdbcArrayTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        JdbcArray array2 = (JdbcArray) rs.getArray(3);
        assertNotNull(array2);
        Object[] a = (Object[]) array2.getArray();
        assertEquals(3, a.length);
        assertEquals(1, a[0]);
        assertEquals(2, a[1]);
        assertEquals(3, a[2]);

        stmt.executeUpdate("DELETE FROM JdbcArrayTest WHERE f1 = 1");

        ps.close();
        stmt.close();
        conn.close();
    }
}
