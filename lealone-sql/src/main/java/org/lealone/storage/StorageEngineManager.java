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
package org.lealone.storage;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.engine.DbSettings;
import org.lealone.message.DbException;

public class StorageEngineManager {

    private StorageEngineManager() {
    }

    private static Map<String, StorageEngine> storageEngines = new ConcurrentHashMap<String, StorageEngine>();
    private static boolean initialized = false;

    public synchronized static void initStorageEngines() {
        if (!initialized)
            initialize();
    }

    public static StorageEngine getStorageEngine(String name) {
        if (!initialized) {
            initialize();
        }
        if (name == null) {
            name = DbSettings.getDefaultSettings().defaultStorageEngine;
        }
        StorageEngine te = storageEngines.get(name.toUpperCase());
        return te;
    }

    public static void registerStorageEngine(StorageEngine storageEngine) {
        storageEngines.put(storageEngine.getName().toUpperCase(), storageEngine);
    }

    public static void deregisterStorageEngine(StorageEngine storageEngine) {
        storageEngines.remove(storageEngine.getName().toUpperCase());
    }

    private static class StorageEngineService implements PrivilegedAction<StorageEngine> {
        @Override
        public StorageEngine run() {
            Iterator<StorageEngine> iterator = ServiceLoader.load(StorageEngine.class).iterator();
            try {
                while (iterator.hasNext()) {
                    //执行next时ServiceLoader内部会自动为每一个实现StorageEngine接口的类生成一个新实例
                    //所以StorageEngine接口的实现类必需有一个public的无参数构造函数
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
        loadStorageEngines();
    }

    private static void loadStorageEngines() {
        AccessController.doPrivileged(new StorageEngineService());
    }
}
