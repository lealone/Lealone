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
package org.lealone.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;

/**
 * 管理所有Database
 * 
 * @author zhh
 */
public class LealoneDatabase extends Database {

    // 所有元数据从名为lealone的数据库开始查找，并且ID固定为0
    public static final int ID = 0;
    public static final String NAME = Constants.PROJECT_NAME;

    private static LealoneDatabase INSTANCE = new LealoneDatabase();

    public static LealoneDatabase getInstance() {
        return INSTANCE;
    }

    private final ConcurrentHashMap<String, Database> databases;;

    private LealoneDatabase() {
        super(ID, NAME, null);
        databases = new ConcurrentHashMap<>();
        databases.put(NAME, this);

        INSTANCE = this; // init执行过程中会触发getInstance()，此时INSTANCE为null，会导致NPE

        init();
        createRootUserIfNotExists();
    }

    // 安全注释见: DatabaseEngine.ServerSessionFactory.createSession(String, ConnectionInfo, boolean)
    Database createDatabase(String dbName, ConnectionInfo ci) {
        String sql = getCreateSQL(quoteIdentifier(dbName), ci);
        getSystemSession().prepareStatementLocal(sql).executeUpdate();
        // 执行完CREATE DATABASE后会加到databases字段中
        // CreateDatabase.update -> Database.addDatabaseObject -> Database.getMap -> this.getDatabasesMap
        Database db = databases.get(dbName);
        if (SysProperties.CHECK) {
            if (db == null) {
                DbException.throwInternalError("not found: " + dbName);
            }
        }
        return db;
    }

    void closeDatabase(String dbName) {
        databases.remove(dbName);
    }

    Map<String, Database> getDatabasesMap() {
        return databases;
    }

    List<Database> getDatabases() {
        return new ArrayList<>(databases.values());
    }

    public Database findDatabase(String dbName) {
        return databases.get(dbName);
    }

    /**
     * Get database with the given name. This method throws an exception if the database
     * does not exist.
     *
     * @param name the database name
     * @return the database
     * @throws DbException if the database does not exist
     */
    public Database getDatabase(String dbName) {
        Database db = findDatabase(dbName);
        if (db == null) {
            throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
        }
        return db;
    }

    @Override
    public synchronized Database copy() {
        INSTANCE = new LealoneDatabase();
        return INSTANCE;
    }
}
