/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.client.jdbc.JdbcBlob;

public class JdbcBlobTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        // setEmbedded(false).enableTrace(TraceSystem.DEBUG);
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

        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO JdbcBlobTest(f1, f2, f3) VALUES(1, 2, ?)");
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
