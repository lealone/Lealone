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

import java.sql.SQLException;
import java.util.Properties;

import org.lealone.client.jdbc.JdbcConnection;
import org.lealone.client.jdbc.JdbcDriver;
import org.lealone.client.storage.ClientStorage;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.Storage;

public class LealoneClient {

    public static JdbcConnection getConnection(String url) throws SQLException {
        return JdbcDriver.getConnection(url);
    }

    public static JdbcConnection getConnection(String url, String user, String password) throws SQLException {
        return JdbcDriver.getConnection(url, user, password);
    }

    public static JdbcConnection getConnection(String url, Properties info) throws SQLException {
        return JdbcDriver.getConnection(url, info);
    }

    public static void getConnectionAsync(String url, AsyncHandler<AsyncResult<JdbcConnection>> handler)
            throws SQLException {
        JdbcDriver.getConnectionAsync(url, handler);
    }

    public static void getConnectionAsync(String url, String user, String password,
            AsyncHandler<AsyncResult<JdbcConnection>> handler) throws SQLException {
        JdbcDriver.getConnectionAsync(url, user, password, handler);
    }

    public static void getConnectionAsync(String url, Properties info,
            AsyncHandler<AsyncResult<JdbcConnection>> handler) throws SQLException {
        JdbcDriver.getConnectionAsync(url, info, handler);
    }

    public static Storage getStorage(String url) {
        // return ClientStorageEngine.getInstance().getStorageBuilder().openStorage();
        return new ClientStorage(url);
    }
}
