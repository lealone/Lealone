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
import org.lealone.client.jdbc.JdbcBlob;
import org.lealone.common.trace.TraceSystem;
import org.lealone.test.sql.SqlTestBase;

public class JdbcBlobTest extends SqlTestBase { // UnitTestBase {
    @Test
    public void run() throws Exception {
        setEmbedded(false).enableTrace(TraceSystem.DEBUG);
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcBlobTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcBlobTest (f1 int, f2 long, f3 blob)");

        JdbcBlob blob = (JdbcBlob) conn.createBlob();

        String blobStr = "blob-test";
        StringBuilder buff = new StringBuilder(1000 * blobStr.length());
        for (int i = 0; i < 1000; i++)
            buff.append(blobStr);

        blobStr = buff.toString();
        // 从1开始
        blob.setBytes(1, blobStr.getBytes());

        PreparedStatement ps = conn.prepareStatement("INSERT INTO JdbcBlobTest(f1, f2, f3) VALUES(1, 2, ?)");
        ps.setBlob(1, blob);
        ps.executeUpdate();

        ResultSet rs = stmt.executeQuery("SELECT f1, f2, f3 FROM JdbcBlobTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        blob = (JdbcBlob) rs.getBlob(3);
        assertNotNull(blob);
        String blobStr2 = new String(blob.getBytes(1, blobStr.length()));
        assertEquals(blobStr2, blobStr);
        // System.out.println("f3=" + blobStr);

        stmt.executeUpdate("DELETE FROM JdbcBlobTest WHERE f1 = 1");

        ps.close();
        stmt.close();
        conn.close();
    }
}
