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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.message.DbException;

public class PluggableEngineManager<T extends PluggableEngine> {

    private final Class<T> pluggableEngineClass;

    protected PluggableEngineManager(Class<T> pluggableEngineClass) {
        this.pluggableEngineClass = pluggableEngineClass;
    }

    private final Map<String, T> pluggableEngines = new ConcurrentHashMap<>();
    private boolean initialized = false;

    public T getEngine(String name) {
        if (name == null)
            throw new NullPointerException("name is null");
        if (!initialized)
            init();
        return pluggableEngines.get(name.toUpperCase());
    }

    public void registerEngine(T pluggableEngine) {
        pluggableEngines.put(pluggableEngine.getName().toUpperCase(), pluggableEngine);
    }

    public void deregisterEngine(T pluggableEngine) {
        pluggableEngines.remove(pluggableEngine.getName().toUpperCase());
    }

    public synchronized void init() {
        if (initialized)
            return;
        initialized = true;
        loadPluggableEngines();
    }

    private void loadPluggableEngines() {
        AccessController.doPrivileged(new PluggableEngineService());
    }

    private class PluggableEngineService implements PrivilegedAction<Void> {
        @Override
        public Void run() {
            Iterator<T> iterator = ServiceLoader.load(pluggableEngineClass).iterator();
            try {
                while (iterator.hasNext()) {
                    // 执行next时ServiceLoader内部会自动为每一个实现PluggableEngine接口的类生成一个新实例
                    // 所以PluggableEngine接口的实现类必需有一个public的无参数构造函数
                    iterator.next();
                }
            } catch (Throwable t) {
                DbException.convert(t);
            }
            return null;
        }
    }
}