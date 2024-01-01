/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.client.jdbc.JdbcClob;

public class JdbcClobTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        // setEmbedded(true).enableTrace(TraceSystem.DEBUG);
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        String clobStr = getClobStr();

        init(stmt);
        insert(conn, clobStr);
        query(stmt, clobStr);

        addClob(stmt, clobStr);
        unique(conn, stmt, clobStr);

        test_FILE_READ(conn, stmt, clobStr);

        // stmt.executeUpdate("DELETE FROM JdbcClobTest WHERE f1 = 1");

        stmt.close();
        conn.close();
    }

    static void init(Statement stmt) throws Exception {
        // stmt.executeUpdate("set DB_CLOSE_DELAY 0");
        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcClobTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcClobTest (f1 int, f2 long, f3 clob) "
                + "parameters(USE_TABLE_LOB_STORAGE=true)");
    }

    static String getClobStr() {
        String clobStr = "clob-test";
        StringBuilder buff = new StringBuilder(1000 * clobStr.length());
        for (int i = 0; i < 1000; i++)
            buff.append(clobStr);

        clobStr = buff.toString();
        return clobStr;
    }

    static void insert(Connection conn, String clobStr) throws Exception {
        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO JdbcClobTest(f1, f2, f3) VALUES(1, 2, ?)");
        ps.setClob(1, createClob(conn, clobStr));
        ps.executeUpdate();
        ps.close();
    }

    static void query(Statement stmt, String clobStr) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT f1, f2, f3 FROM JdbcClobTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        JdbcClob clob = (JdbcClob) rs.getClob(3);
        assertNotNull(clob);
        String clobStr2 = clob.getSubString(1, clobStr.length());
        assertEquals(clobStr2, clobStr);
        // System.out.println("f3=" + clobStr);
    }

    static void addClob(Statement stmt, String clobStr) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS AddClobTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS AddClobTest (f1 int, f2 long)");
        stmt.executeUpdate("INSERT INTO AddClobTest(f1, f2) VALUES(1, 2)");
        stmt.executeUpdate("ALTER TABLE AddClobTest ADD f3 clob");
        ResultSet rs = stmt.executeQuery("SELECT f1, f2, f3 FROM AddClobTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));

        JdbcClob clob = (JdbcClob) rs.getClob(3);
        assertNull(clob);
    }

    static JdbcClob createClob(Connection conn, String clobStr) throws Exception {
        JdbcClob clob = (JdbcClob) conn.createClob();
        // 从1开始
        clob.setString(1, clobStr);
        return clob;
    }

    static void unique(Connection conn, Statement stmt, String clobStr) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS UniqueClobTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS UniqueClobTest (f1 int, f2 clob,"
                + "CONSTRAINT uk_configinfo_datagrouptenant UNIQUE (f1))");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO UniqueClobTest(f1, f2) VALUES(?, ?)");
        ps.setInt(1, 1);
        ps.setClob(2, createClob(conn, clobStr));
        ps.executeUpdate();

        ps.setInt(1, 1);
        ps.setClob(2, createClob(conn, clobStr));
        try {
            ps.executeUpdate();
            fail();
        } catch (Exception e) {
            ps = conn.prepareStatement("UPDATE UniqueClobTest SET f2=? WHERE f1=?");
            ps.setInt(2, 1);
            ps.setClob(1, createClob(conn, clobStr));
            ps.executeUpdate();
        }
        ps.close();
    }

    static void test_FILE_READ(Connection conn, Statement stmt, String clobStr) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS test_FILE_READ");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test_FILE_READ (f1 int, f2 clob)");

        String file = "/lealone-test.yaml";
        file = new File(JdbcClobTest.class.getResource(file).toURI()).getCanonicalPath();
        stmt.executeUpdate(
                "INSERT INTO test_FILE_READ(f1, f2) VALUES(1, FILE_READ('" + file + "', NULL))");
        int count = stmt.executeUpdate("UPDATE test_FILE_READ SET f2 = FILE_READ('" + file + "', NULL)");
        assertEquals(1, count);
        ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM test_FILE_READ");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        JdbcClob clob = (JdbcClob) rs.getClob(2);
        assertNotNull(clob);
        rs.close();
    }
}
