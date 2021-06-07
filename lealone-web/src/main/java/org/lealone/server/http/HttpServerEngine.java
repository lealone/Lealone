/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.http;

import java.util.Map;

import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngineBase;

public class HttpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "HTTP";
    private final HttpServer httpServer = new HttpServer();

    public HttpServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return httpServer;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        httpServer.init(config);
    }

    @Override
    public void close() {
        httpServer.stop();
    }
}
