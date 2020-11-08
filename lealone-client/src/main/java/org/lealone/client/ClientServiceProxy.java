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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

public class ClientServiceProxy {

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
