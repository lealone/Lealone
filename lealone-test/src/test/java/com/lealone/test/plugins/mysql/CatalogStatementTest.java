/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class CatalogStatementTest extends MySQLTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("create catalog if not exists my_catalog");
        ResultSet rs = stmt.executeQuery("SHOW CATALOGS");
        boolean found = false;
        while (rs.next()) {
            if (rs.getString(1).equalsIgnoreCase("my_catalog")) {
                found = true;
                break;
            }
        }
        rs.close();
        assertTrue(found);
        Connection conn = getMySQLConnection("my_catalog.information_schema");
        Statement s = conn.createStatement();
        rs = s.executeQuery("SHOW TABLES");
        while (rs.next()) {
            assertTrue(rs.getString(3).equalsIgnoreCase("my_catalog"));
        }
        rs.close();
        s.close();
        conn.close();
    }
}
