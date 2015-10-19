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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.db.auth.Auth;

/**
 * 
 * 管理所有Database，并负责Auth的初始化
 * 
 * @author zhh
 */
public class LealoneDatabase extends Database {

    public static final String NAME = Constants.PROJECT_NAME;

    private static final LealoneDatabase INSTANCE = new LealoneDatabase();

    public static LealoneDatabase getInstance() {
        if (!INSTANCE.isInitialized()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInitialized())
                    INSTANCE.init();
            }
        }
        return INSTANCE;
    }

    private final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();

    private LealoneDatabase() {
        super(0, NAME, null);
        databases.put(NAME, this);
    }

    private synchronized void init() {
        String url = Constants.URL_PREFIX + Constants.URL_EMBED + NAME;
        ConnectionInfo ci = new ConnectionInfo(url, (Properties) null);
        ci.setBaseDir(SysProperties.getBaseDir());
        init(ci);

        if (Auth.getAllUsers().isEmpty()) {
            getSystemSession().prepareStatementLocal("CREATE USER IF NOT EXISTS lealone PASSWORD 'lealone' ADMIN")
                    .executeUpdate();
            getSystemSession().prepareStatementLocal("CREATE USER IF NOT EXISTS sa PASSWORD '' ADMIN").executeUpdate();
        }
    }

    @Override
    protected void initTraceSystem(ConnectionInfo ci) {
        super.initTraceSystem(ci);
        // Auth里的User、Role用到TraceSystem，初始化TraceSystem后才能初始化Auth
        Auth.init(this);
    }

    public Database findDatabase(String dbName) {
        return databases.get(dbName);
    }

    synchronized Database createDatabase(String dbName, ConnectionInfo ci) {
        String sql = getSQL(quoteIdentifier(dbName), ci);
        getSystemSession().prepareStatementLocal(sql).executeUpdate();
        Database db = databases.get(dbName);
        return db;
    }

    void closeDatabase(String dbName) {
        databases.remove(dbName);
    }

    List<Database> getDatabases() {
        return new ArrayList<>(databases.values());
    }

    Map<String, Database> getDatabasesMap() {
        return databases;
    }
}
