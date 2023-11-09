/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.db.PluginBase;
import org.lealone.db.PluginManager;

public abstract class ProtocolServerEngineBase extends PluginBase implements ProtocolServerEngine {

    protected boolean inited;
    protected ProtocolServer protocolServer;

    public ProtocolServerEngineBase(String name) {
        super(name);
    }

    protected abstract ProtocolServer createProtocolServer();

    @Override
    public ProtocolServer getProtocolServer() {
        if (protocolServer == null)
            protocolServer = createProtocolServer();
        return protocolServer;
    }

    @Override
    public void init(Map<String, String> config) {
        getProtocolServer().init(config);
        inited = true;
    }

    @Override
    public boolean isInited() {
        return inited;
    }

    @Override
    public void close() {
        stop();
        PluginManager.deregister(this);
    }

    @Override
    public void start() {
        getProtocolServer().start();
    }

    @Override
    public void stop() {
        getProtocolServer().stop();
    }
}
