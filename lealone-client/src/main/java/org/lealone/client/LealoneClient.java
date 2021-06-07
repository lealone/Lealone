/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client;

import java.util.Properties;

import org.lealone.client.jdbc.JdbcConnection;
import org.lealone.client.jdbc.JdbcDriver;
import org.lealone.db.async.Future;

public class LealoneClient {

    public static Future<JdbcConnection> getConnection(String url) {
        return JdbcDriver.getConnection(url);
    }

    public static Future<JdbcConnection> getConnection(String url, String user, String password) {
        return JdbcDriver.getConnection(url, user, password);
    }

    public static Future<JdbcConnection> getConnection(String url, Properties info) {
        return JdbcDriver.getConnection(url, info);
    }
}
