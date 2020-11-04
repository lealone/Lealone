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
package org.lealone.client;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

public class ClientServiceProxy {

    // 不需要加{}，避免做字符串翻译操作
    private static final String sqlNoReturnValue = "call EXECUTE_SERVICE_NO_RETURN_VALUE(?,?)";
    private static final String sqlWithReturnValue = "? = call EXECUTE_SERVICE_WITH_RETURN_VALUE(?,?)";

    public static String executeWithReturnValue(String url, String serviceName, String json) {
        try (Connection conn = DriverManager.getConnection(url);
                CallableStatement stmt = conn.prepareCall(sqlWithReturnValue)) {
            stmt.setString(2, serviceName);
            stmt.setString(3, json);
            stmt.registerOutParameter(1, java.sql.Types.VARCHAR);
            if (stmt.execute()) {
                return stmt.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute service: " + serviceName, e);
        }
        return null;
    }

    public static void executeNoReturnValue(String url, String serviceName, String json) {
        try (Connection conn = DriverManager.getConnection(url);
                CallableStatement stmt = conn.prepareCall(sqlNoReturnValue)) {
            stmt.setString(1, serviceName);
            stmt.setString(2, json);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute service: " + serviceName, e);
        }
    }

    private static HashMap<String, Connection> connMap = new HashMap<>(1);

    public static PreparedStatement prepareStatement(String url, String sql) {
        try {
            Connection conn = connMap.get(url);
            if (conn == null) {
                synchronized (connMap) {
                    conn = connMap.get(url);
                    if (conn == null) {
                        conn = DriverManager.getConnection(url);
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
}
