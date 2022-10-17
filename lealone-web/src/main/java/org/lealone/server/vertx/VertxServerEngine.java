/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.vertx;

import java.util.Map;

import org.lealone.server.ProtocolServer;
import org.lealone.server.http.HttpServerEngine;

public class VertxServerEngine extends HttpServerEngine {

    public static final String NAME = "vertx";
    private VertxServer server; // 延迟初始化

    public VertxServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        if (server == null)
            server = new VertxServer();
        return server;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        getProtocolServer().init(config);
    }

    @Override
    public void close() {
        getProtocolServer().stop();
    }
}
