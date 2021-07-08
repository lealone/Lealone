/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

public abstract class PluggableEngineManager<T extends PluggableEngine> {

    private static final Logger logger = LoggerFactory.getLogger(PluggableEngineManager.class);

    private final Class<T> pluggableEngineClass;
    private final Map<String, T> pluggableEngines = new ConcurrentHashMap<>();
    private volatile boolean loaded = false;

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
        AccessController.doPrivileged(new PluggableEngineService());
        // 注意在load完之后再设为true，否则其他线程可能会因为不用等待load完成从而得到一个NPE
        loaded = true;
    }

    private class PluggableEngineService implements PrivilegedAction<Void> {
        @Override
        public Void run() {
            try {
                // 执行next时ServiceLoader内部会自动为每一个实现PluggableEngine接口的类生成一个新实例
                // 所以PluggableEngine接口的实现类必需有一个public的无参数构造函数
                for (T e : ServiceLoader.load(pluggableEngineClass)) {
                    PluggableEngineManager.this.registerEngine(e);
                }
            } catch (Throwable t) {
                // 只是发出警告
                logger.warn("Failed to load pluggable engine: " + pluggableEngineClass.getName(), t);
                // DbException.convert(t);
            }
            return null;
        }
    }
}
