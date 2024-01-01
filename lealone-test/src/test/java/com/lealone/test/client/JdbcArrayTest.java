/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.client.jdbc.JdbcArray;

public class JdbcArrayTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcArrayTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcArrayTest (f1 int, f2 long, f3 array)");

        JdbcArray array = (JdbcArray) conn.createArrayOf(null, new Object[] { 1, 2, 3 });

        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO JdbcArrayTest(f1, f2, f3) VALUES(1, 2, ?)");
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
