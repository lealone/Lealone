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
package org.lealone.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.lealone.jdbc.JdbcConnection;
import org.lealone.message.DbException;
import org.lealone.util.JdbcUtils;
import org.lealone.util.New;

public class SystemDatabase {

    public static final String NAME = "system";

    private static JdbcConnection conn;
    private static PreparedStatement addDatabase;
    private static PreparedStatement removeDatabase;
    private static PreparedStatement findOne;
    private static PreparedStatement findAll;

    static synchronized void init(String baseDir) {
        if (conn == null) {
            if (baseDir == null) {
                baseDir = SysProperties.getBaseDir();
            }

            String url = Constants.URL_PREFIX + Constants.URL_EMBED + NAME;
            ConnectionInfo ci = new ConnectionInfo(url, NAME);
            if (baseDir != null)
                ci.setBaseDir(baseDir);
            ci.setUserName("DBA");
            ci.setUserPasswordHash(new byte[0]);
            ci.setProperty("DEFAULT_STORAGE_ENGINE", Constants.DEFAULT_STORAGE_ENGINE_NAME);

            Statement stmt = null;
            try {
                conn = new JdbcConnection(ci, false);
                stmt = conn.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS databases" //
                        + "(db_name VARCHAR, storage_engine_name VARCHAR, create_time TIMESTAMP, "//
                        + "PRIMARY KEY(db_name))");
                addDatabase = conn.prepareStatement("INSERT INTO databases VALUES(?, ?, NOW())");
                removeDatabase = conn.prepareStatement("DELETE FROM databases WHERE db_name=?");
                findOne = conn.prepareStatement("SELECT * FROM databases WHERE db_name=?");
                findAll = conn.prepareStatement("SELECT db_name FROM databases");
            } catch (SQLException e) {
                throw DbException.convert(e);
            } finally {
                JdbcUtils.closeSilently(stmt);
            }
        }
    }

    public static Connection getConnection() {
        return conn;
    }

    public static List<String> findAll() {
        List<String> dbNames = New.arrayList();
        if (conn == null)
            return dbNames;

        ResultSet rs = null;
        try {
            rs = findAll.executeQuery();
            while (rs.next()) {
                dbNames.add(rs.getString(1));
            }

            return dbNames;
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
        }

    }

    public static synchronized void addDatabase(String dbName, String storageEngineName) {
        if (conn == null || exists(dbName))
            return;
        try {
            addDatabase.setString(1, dbName);
            addDatabase.setString(2, storageEngineName);
            addDatabase.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public static synchronized void removeDatabase(String dbName) {
        if (conn == null)
            return;
        try {
            removeDatabase.setString(1, dbName);
            removeDatabase.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public static boolean exists(String dbName) {
        if (conn == null)
            return false;
        ResultSet rs = null;
        try {
            findOne.setString(1, dbName);
            rs = findOne.executeQuery();
            if (rs.next()) {
                return true;
            }

            return false;
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
        }

    }
}
