/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mysql;

import java.sql.ResultSet;

import org.junit.Test;

public class MySQLShowStatementTest extends MySQLTestBase {
    @Test
    public void run() throws Exception {
        ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'lower_case_%'");
        rs.next();
    }
}
