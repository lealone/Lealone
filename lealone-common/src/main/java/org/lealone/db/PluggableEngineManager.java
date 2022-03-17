/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.Collection;

public abstract class PluggableEngineManager<T extends PluggableEngine> extends PluginManager<T> {

    protected PluggableEngineManager(Class<T> pluggableEngineClass) {
        super(pluggableEngineClass);
    }

    public T getEngine(String name) {
        return getPlugin(name);
    }

    public Collection<T> getEngines() {
        return getPlugins();
    }

    public void registerEngine(T pluggableEngine, String... alias) {
        registerPlugin(pluggableEngine, alias);
    }

    public void deregisterEngine(T pluggableEngine, String... alias) {
        deregisterPlugin(pluggableEngine, alias);
    }
}
