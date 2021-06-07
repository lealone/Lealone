/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

public abstract class ProtocolServerEngineBase implements ProtocolServerEngine {

    protected final String name;
    protected boolean inited;

    // 目前用不到
    // protected Map<String, String> config;

    public ProtocolServerEngineBase(String name) {
        this.name = name;
        // 见PluggableEngineManager.PluggableEngineService中的注释
        ProtocolServerEngineManager.getInstance().registerEngine(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init(Map<String, String> config) {
        // this.config = config;
        inited = true;
    }

    @Override
    public boolean isInited() {
        return inited;
    }

    @Override
    public void close() {
    }

}
