/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.Connection;
import java.sql.SQLException;

import com.lealone.client.LealoneClient;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.async.Future;
import com.lealone.test.TestBase;
import com.lealone.test.TestBase.MainTest;

public class LealoneClientTest implements MainTest {

    public static void main(String[] args) {
        String url = new TestBase().getURL(LealoneDatabase.NAME);
        connectionTest(url);
    }

    static void connectionTest(String url) {
        try {
            Connection conn = LealoneClient.getConnection(url).get();
            for (int i = 0; i < 5; i++) {
                connectionTest(url, i);
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    private static void connectionTest(String url, int loop) throws SQLException {
        int connectionCount = 10000;
        Connection[] connections = new Connection[connectionCount];
        Future[] futures = new Future[connectionCount];
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < connectionCount; i++) {
            int index = i;
            futures[index] = LealoneClient.getConnection(url).onSuccess(c -> {
                connections[index] = c;
            });
        }
        for (int i = 0; i < connectionCount; i++) {
            futures[i].get();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("loop: " + loop + ", connection count: " + connectionCount + ", total time: "
                + (t2 - t1) + " ms" + ", avg time: " + (t2 - t1) / (connectionCount * 1.0) + " ms");
        for (int i = 0; i < connectionCount; i++) {
            connections[i].close();
        }
    }
}
