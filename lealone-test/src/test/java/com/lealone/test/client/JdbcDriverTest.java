/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.lealone.client.jdbc.JdbcDriver;
import com.lealone.db.LealoneDatabase;

public class JdbcDriverTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        String url = getURL(LealoneDatabase.NAME);

        new Thread(() -> {
            JdbcDriver.getConnection(url).get();
        }).start();

        new Thread(() -> {
            JdbcDriver.getConnection(url).get();
        }).start();

        JdbcDriver.getConnection(url).onSuccess(conn -> {
            assertNotNull(conn);
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).get();

        try {
            JdbcDriver.getConnection("invalid url").onComplete(ar -> {
                assertTrue(ar.isFailed());
                assertNotNull(ar.getCause());
            }).get(); // get方法会抛出异常
            fail();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
        }

        Connection conn = JdbcDriver.getConnection(url).get();
        assertNotNull(conn);
        conn.close();
    }
}
