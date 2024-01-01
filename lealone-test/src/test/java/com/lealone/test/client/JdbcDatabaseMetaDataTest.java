/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.junit.Test;

public class JdbcDatabaseMetaDataTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        // enableTrace(TraceSystem.DEBUG);
        Connection conn = getConnection();
        DatabaseMetaData md = conn.getMetaData();
        System.out.println(md.getDatabaseMajorVersion());
        System.out.println(md.getDatabaseProductVersion());
        ResultSet rs = md.getCatalogs();
        printResultSet(rs);
        rs.close();
    }
}
