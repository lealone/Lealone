/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.lealone.db.SysProperties;
import org.lealone.test.UnitTestBase;

// 跑玩JdbcClobTest后，停掉tcp server再测
public class JdbcClobEmbedTest extends UnitTestBase {
    // @Test
    public void run() throws Exception {
        String clobStr = JdbcClobTest.getClobStr();
        SysProperties.setBaseDir("target/test-data/client-server");
        String url = "jdbc:lealone:embed:" + DEFAULT_DB_NAME;
        Connection conn = DriverManager.getConnection(url, DEFAULT_USER, DEFAULT_PASSWORD);
        Statement stmt = conn.createStatement();
        JdbcClobTest.query(stmt, clobStr);
        stmt.close();
        conn.close();
    }
}
