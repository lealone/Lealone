/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.junit.Test;

public class JdbcResultSetMetaDataTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcResultSetMetaDataTest");
        stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS JdbcResultSetMetaDataTest (f1 int, f2 ENUM('a','b'))");

        ResultSet rs = stmt.executeQuery("SELECT f2 FROM JdbcResultSetMetaDataTest");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("ENUM", md.getColumnTypeName(1));
        assertEquals(String.class.getName(), md.getColumnClassName(1));
        rs.close();
    }
}
