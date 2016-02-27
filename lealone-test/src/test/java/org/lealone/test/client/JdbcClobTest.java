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
import org.lealone.client.jdbc.JdbcClob;
import org.lealone.common.trace.TraceSystem;
import org.lealone.test.UnitTestBase;

public class JdbcClobTest extends UnitTestBase {
    @Test
    public void run() throws Exception {
        setEmbedded(true).enableTrace(TraceSystem.DEBUG);
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcClobTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcClobTest (f1 int, f2 long, f3 clob)");

        JdbcClob clob = (JdbcClob) conn.createClob();

        String clobStr = "clob-test";
        StringBuilder buff = new StringBuilder(1000 * clobStr.length());
        for (int i = 0; i < 1000; i++)
            buff.append(clobStr);

        clobStr = buff.toString();
        // 从1开始
        clob.setString(1, clobStr);

        PreparedStatement ps = conn.prepareStatement("INSERT INTO JdbcClobTest(f1, f2, f3) VALUES(1, 2, ?)");
        ps.setClob(1, clob);
        ps.executeUpdate();

        ResultSet rs = stmt.executeQuery("SELECT f1, f2, f3 FROM JdbcClobTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        clob = (JdbcClob) rs.getClob(3);
        assertNotNull(clob);
        clobStr = clob.getSubString(1, clobStr.length());
        System.out.println("f3=" + clobStr);

        stmt.executeUpdate("DELETE FROM JdbcClobTest WHERE f1 = 1");

        ps.close();
        stmt.close();
        conn.close();
    }
}
