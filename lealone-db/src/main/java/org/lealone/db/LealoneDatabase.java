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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveConcurrentHashMap;

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
        databases = new CaseInsensitiveConcurrentHashMap<>();
        databases.put(NAME, this);

        INSTANCE = this; // init执行过程中会触发getInstance()，此时INSTANCE为null，会导致NPE

        init();
        createRootUserIfNotExists();

        DatabaseManager.setDatabases(databases);
    }

    public synchronized Database createEmbeddedDatabase(String name, ConnectionInfo ci) {
        Database db = databases.get(name);
        if (db != null)
            return db;

        HashMap<String, String> parameters = new HashMap<>();
        for (Entry<Object, Object> e : ci.getProperties().entrySet()) {
            parameters.put(e.getKey().toString(), e.getValue().toString());
        }
        int id = INSTANCE.allocateObjectId();
        db = new Database(id, name, parameters);
        db.setRunMode(RunMode.EMBEDDED);
        db.init();
        String userName = ci.getUserName();
        byte[] userPasswordHash = ci.getUserPasswordHash();
        db.createAdminUser(userName, userPasswordHash);
        addDatabaseObject(getSystemSession(), db);
        getSystemSession().commit();
        return db;
    }

    void closeDatabase(String dbName) {
        databases.remove(dbName);
    }

    Map<String, Database> getDatabasesMap() {
        return databases;
    }

    public List<Database> getDatabases() {
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
