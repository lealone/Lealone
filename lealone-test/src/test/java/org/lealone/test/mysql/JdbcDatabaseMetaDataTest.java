/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.mysql;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.junit.Test;

public class JdbcDatabaseMetaDataTest extends MySQLTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("create schema if not exists test");
        executeUpdate("use test");
        executeUpdate("create table if not exists VVV(f1 int)");
        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs = md.getCatalogs();
        printResultSet(rs);
        rs.close();
        rs = md.getTables(null, "test", "vvv", null);
        printResultSet(rs);
        rs.close();
        rs = md.getTables(null, "test", "VVV", null);
        printResultSet(rs);
        rs.close();
    }
}
