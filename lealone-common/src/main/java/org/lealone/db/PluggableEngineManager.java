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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.exceptions.DbException;

public abstract class PluggableEngineManager<T extends PluggableEngine> {

    private final Class<T> pluggableEngineClass;
    private final Map<String, T> pluggableEngines = new ConcurrentHashMap<>();
    private boolean loaded = false;

    protected PluggableEngineManager(Class<T> pluggableEngineClass) {
        this.pluggableEngineClass = pluggableEngineClass;
    }

    public T getEngine(String name) {
        if (name == null)
            throw new NullPointerException("name is null");
        if (!loaded)
            loadPluggableEngines();
        return pluggableEngines.get(name.toUpperCase());
    }

    public Collection<T> getEngines() {
        return pluggableEngines.values();
    }

    public void registerEngine(T pluggableEngine, String... alias) {
        pluggableEngines.put(pluggableEngine.getName().toUpperCase(), pluggableEngine);
        pluggableEngines.put(pluggableEngine.getClass().getName().toUpperCase(), pluggableEngine);
        if (alias != null) {
            for (String a : alias)
                pluggableEngines.put(a.toUpperCase(), pluggableEngine);
        }
    }

    public void deregisterEngine(T pluggableEngine, String... alias) {
        pluggableEngines.remove(pluggableEngine.getName().toUpperCase());
        pluggableEngines.remove(pluggableEngine.getClass().getName().toUpperCase().toUpperCase());
        if (alias != null) {
            for (String a : alias)
                pluggableEngines.remove(a.toUpperCase());
        }
    }

    private synchronized void loadPluggableEngines() {
        if (loaded)
            return;
        loaded = true;
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
