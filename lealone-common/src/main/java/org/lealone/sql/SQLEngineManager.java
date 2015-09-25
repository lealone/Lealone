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
package org.lealone.sql;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.message.DbException;
import org.lealone.db.DbSettings;

public class SQLEngineManager {

    private SQLEngineManager() {
    }

    private static Map<String, SQLEngine> sqlEngines = new ConcurrentHashMap<String, SQLEngine>();
    private static boolean initialized = false;

    public synchronized static void initSQLEngines() {
        if (!initialized)
            initialize();
    }

    public static SQLEngine getSQLEngine(String name) {
        if (!initialized) {
            initialize();
        }
        if (name == null) {
            name = DbSettings.getDefaultSettings().defaultSQLEngine;
        }
        SQLEngine te = sqlEngines.get(name.toUpperCase());
        return te;
    }

    public static void registerSQLEngine(SQLEngine sqlEngine) {
        sqlEngines.put(sqlEngine.getName().toUpperCase(), sqlEngine);
    }

    public static void deregisterSQLEngine(SQLEngine sqlEngine) {
        sqlEngines.remove(sqlEngine.getName().toUpperCase());
    }

    private static class SQLEngineService implements PrivilegedAction<SQLEngine> {
        @Override
        public SQLEngine run() {
            Iterator<SQLEngine> iterator = ServiceLoader.load(SQLEngine.class).iterator();
            try {
                while (iterator.hasNext()) {
                    // 执行next时ServiceLoader内部会自动为每一个实现SQLEngine接口的类生成一个新实例
                    // 所以SQLEngine接口的实现类必需有一个public的无参数构造函数
                    iterator.next();
                }
            } catch (Throwable t) {
                DbException.convert(t);
            }
            return null;
        }
    }

    private synchronized static void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;
        loadSQLEngines();
    }

    private static void loadSQLEngines() {
        AccessController.doPrivileged(new SQLEngineService());
    }
}
