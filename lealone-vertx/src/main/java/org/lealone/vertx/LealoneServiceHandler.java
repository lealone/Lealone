/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.vertx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.client.jdbc.JdbcConnection;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.ServerSession;
import org.lealone.db.service.ServiceExecuterManager;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

public class LealoneServiceHandler implements Handler<SockJSSocket> {

    private static final Logger logger = LoggerFactory.getLogger(LealoneServiceHandler.class);
    private static final ConcurrentSkipListMap<Integer, Connection> currentConnections = new ConcurrentSkipListMap<>();

    private static void removeConnection(Integer key) {
        currentConnections.remove(key);
    }

    @Override
    public void handle(SockJSSocket sockJSSocket) {
        sockJSSocket.endHandler(v -> {
            removeConnection(sockJSSocket.hashCode());
        });
        sockJSSocket.exceptionHandler(t -> {
            removeConnection(sockJSSocket.hashCode());
            logger.error("sockJSSocket exception", t);
        });

        sockJSSocket.handler(buffer -> {
            LealoneServiceHandler.handle(sockJSSocket, buffer.getString(0, buffer.length()));
        });
    }

    public static Buffer handle(Object lock, String command) {
        String a[] = command.split(";");
        int type = Integer.parseInt(a[0]);
        if (type < 500) {
            return executeService(a, type);
        } else {
            return executeSql(lock, a, type);
        }
    }

    private static Buffer executeService(String a[], int type) {
        String serviceName = CamelCaseHelper.toUnderscoreFromCamel(a[1]);
        String json = null;
        if (a.length >= 3) {
            json = a[2];
        }
        JsonArray ja = new JsonArray();
        String result = null;
        switch (type) {
        case 1:
            try {
                result = ServiceExecuterManager.executeServiceWithReturnValue(serviceName, json);
                ja.add(2);
            } catch (Exception e) {
                ja.add(3);
                result = "failed to execute service: " + serviceName + ", cause: " + e.getMessage();
                logger.error(result, e);
            }
            break;
        default:
            ja.add(3);
            result = "unknown request type: " + type + ", serviceName: " + serviceName;
            logger.error(result);
        }
        ja.add(a[1]); // 前端传来的方法名不一定是下划线风格的，所以用最初的
        ja.add(result);
        return Buffer.buffer(ja.toString());
    }

    private static Buffer executeSql(Object lock, String a[], int type) {
        JsonArray ja = new JsonArray();
        try {
            Connection conn;
            synchronized (lock) {
                conn = currentConnections.get(lock.hashCode());
                if (conn == null) {
                    String url = System.getProperty("lealone.jdbc.url");
                    if (url == null) {
                        throw new RuntimeException("'lealone.jdbc.url' must be set");
                    }

                    conn = DriverManager.getConnection(url);
                    currentConnections.put(lock.hashCode(), conn);
                }
            }
            PreparedStatement ps = null;
            if (a.length > 2) {
                ps = conn.prepareStatement(a[2]);
                JsonArray parms = new JsonArray(a[3]);
                for (int i = 0, size = parms.size(); i < size; i++) {
                    ps.setObject(i + 1, parms.getValue(i));
                }
            }
            String result = "ok";
            ja.add(type);
            ja.add(a[1]);
            switch (type) {
            case 500: {
                ps.executeUpdate();
                long rowId = ((ServerSession) ((JdbcConnection) conn).getSession()).getLastIdentity().getLong();
                ja.add(rowId);
                break;
            }
            case 501:
            case 502:
                int count = ps.executeUpdate();
                ja.add(count);
                break;
            case 503: {
                ResultSet rs = ps.executeQuery();
                HashMap<String, Object> map = new HashMap<>();
                if (rs.next()) {
                    ResultSetMetaData md = rs.getMetaData();
                    for (int i = 1, len = md.getColumnCount(); i <= len; i++) {
                        map.put(md.getColumnName(i), rs.getString(i));
                    }
                }
                JsonObject jo = new JsonObject(map);
                ja.add(jo);
                break;
            }
            case 504: {
                ResultSet rs = ps.executeQuery();
                ResultSetMetaData md = rs.getMetaData();
                JsonArray records = new JsonArray();
                while (rs.next()) {
                    HashMap<String, Object> map = new HashMap<>();
                    for (int i = 1, len = md.getColumnCount(); i <= len; i++) {
                        map.put(md.getColumnName(i), rs.getString(i));
                    }
                    JsonObject jo = new JsonObject(map);
                    records.add(jo);
                }
                ja.add(records);
                break;
            }
            case 601:
                conn.setAutoCommit(false);
                ja.add(result);
                break;
            case 602:
                conn.commit();
                conn.setAutoCommit(true);
                ja.add(result);
                break;
            case 603:
                conn.rollback();
                conn.setAutoCommit(true);
                ja.add(result);
                break;
            default:
                ja.add(3);
                result = "unknown request type: " + type + ", sql: " + a[2];
                logger.error(result);
            }
        } catch (SQLException e) {
            ja.add(3);
            logger.error(e);
        }
        return Buffer.buffer(ja.toString());
    }
}
