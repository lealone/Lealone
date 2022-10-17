/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import java.util.Map;

import org.lealone.server.ProtocolServer;
import org.lealone.server.http.HttpServerEngine;

public class TomcatServerEngine extends HttpServerEngine {

    public static final String NAME = "tomcat";
    private TomcatServer tomcatServer; // 延迟初始化

    public TomcatServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        if (tomcatServer == null)
            tomcatServer = new TomcatServer();
        return tomcatServer;
    }

    @Override
    public void init(Map<String, String> config) {
        getProtocolServer().init(config);
    }

    @Override
    public void close() {
        getProtocolServer().stop();
    }
}
