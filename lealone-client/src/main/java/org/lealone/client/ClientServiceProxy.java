/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.Constants;

public class ClientServiceProxy {

    private static ConcurrentHashMap<String, Connection> connMap = new ConcurrentHashMap<>(1);

    public static PreparedStatement prepareStatement(String url, String sql) {
        try {
            Connection conn = connMap.get(url);
            if (conn == null) {
                synchronized (connMap) {
                    conn = connMap.get(url);
                    if (conn == null) {
                        Properties info = new Properties();
                        info.put(ConnectionSetting.IS_SERVICE_CONNECTION.name(), "true");
                        conn = LealoneClient.getConnection(url, info).get();
                        connMap.put(url, conn);
                    }
                }
            }
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare statement: " + sql, e);
        }
    }

    public static RuntimeException failed(String serviceName, Throwable cause) {
        return new RuntimeException("Failed to execute service: " + serviceName, cause);
    }

    public static boolean isEmbedded(String url) {
        return url == null || new ConnectionInfo(url).isEmbedded();
    }

    public static String getUrl() {
        return System.getProperty(Constants.JDBC_URL_KEY);
    }
}
