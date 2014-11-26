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
package org.lealone.dbobject.table;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.api.TableEngine;
import org.lealone.engine.DbSettings;
import org.lealone.message.DbException;

public class TableEngineManager {

    private TableEngineManager() {
    }

    private static Map<String, TableEngine> tableEngines = new ConcurrentHashMap<String, TableEngine>();
    private static boolean initialized = false;

    public static TableEngine getTableEngine(String name) {
        if (!initialized) {
            initialize();
        }
        if (name == null) {
            name = DbSettings.getInstance().defaultTableEngine;
            //TODO 如果为null怎么办？抛错？还是设个默认值，默认值从哪取？
            //if (name == null)
            //    name = MemoryTableEngine.NAME;
        }
        TableEngine te = tableEngines.get(name.toUpperCase());
        //        if (te == null)
        //            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, name + " store engine not found");
        return te;
    }

    public static void registerTableEngine(TableEngine tableEngine) {
        tableEngines.put(tableEngine.getName().toUpperCase(), tableEngine);
    }

    public static void deregisterTableEngine(TableEngine tableEngine) {
        tableEngines.remove(tableEngine.getName().toUpperCase());
    }

    private static class TableEngineService implements PrivilegedAction<TableEngine> {
        @Override
        public TableEngine run() {
            Iterator<TableEngine> iterator = ServiceLoader.load(TableEngine.class).iterator();
            try {
                while (iterator.hasNext()) {
                    //执行next时ServiceLoader内部会自动为每一个实现TableEngine接口的类生成一个新实例
                    //所以TableEngine接口的实现类必需有一个public的无参数构造函数
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
        loadTableEngines();
    }

    private static void loadTableEngines() {
        AccessController.doPrivileged(new TableEngineService());
    }
}
