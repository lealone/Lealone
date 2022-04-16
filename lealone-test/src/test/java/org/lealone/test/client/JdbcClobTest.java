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
import org.lealone.client.jdbc.JdbcClob;

public class JdbcClobTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        // setEmbedded(true).enableTrace(TraceSystem.DEBUG);
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        // stmt.executeUpdate("set DB_CLOSE_DELAY 0");
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
        String clobStr2 = clob.getSubString(1, clobStr.length());
        assertEquals(clobStr2, clobStr);
        // System.out.println("f3=" + clobStr);

        stmt.executeUpdate("DELETE FROM JdbcClobTest WHERE f1 = 1");

        ps.close();
        stmt.close();
        conn.close();
    }
}
