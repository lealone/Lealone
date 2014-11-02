/*
 * Copyright 2011 The Apache Software Foundation
 *
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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.message.DbException;

public class DatabaseEngineManager {
    private DatabaseEngineManager() {
    }

    private static Map<String, DatabaseEngine> dbEngines = new ConcurrentHashMap<String, DatabaseEngine>();
    private static boolean initialized = false;

    public static DatabaseEngine getDatabaseEngine(String name) {
        if (!initialized) {
            initialize();
        }

        return dbEngines.get(name.toUpperCase());
    }

    public static void registerDatabaseEngine(DatabaseEngine dbEngine) {
        dbEngines.put(dbEngine.getName().toUpperCase(), dbEngine);
    }

    public static void deregisterDatabaseEngine(DatabaseEngine dbEngine) {
        dbEngines.remove(dbEngine.getName().toUpperCase());
    }

    private static class DatabaseEngineService implements PrivilegedAction<DatabaseEngine> {
        public DatabaseEngine run() {
            Iterator<DatabaseEngine> iterator = ServiceLoader.load(DatabaseEngine.class).iterator();
            try {
                while (iterator.hasNext()) {
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
        loadDatabaseEngines();
    }

    private static void loadDatabaseEngines() {
        AccessController.doPrivileged(new DatabaseEngineService());
    }
}
