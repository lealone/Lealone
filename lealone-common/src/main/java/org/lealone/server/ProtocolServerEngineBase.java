/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;

public abstract class ProtocolServerEngineBase extends PluginBase implements ProtocolServerEngine {

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
        super.init(config);
        getProtocolServer().init(config);
    }

    @Override
    public void close() {
        stop();
        super.close();
    }

    @Override
    public void start() {
        getProtocolServer().start();
        super.start();
    }

    @Override
    public void stop() {
        getProtocolServer().stop();
        super.stop();
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return ProtocolServerEngine.class;
    }
}
