/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import com.lealone.server.ProtocolServer;
import com.lealone.service.http.HttpServerEngine;

public class TomcatServerEngine extends HttpServerEngine {

    public static final String NAME = "HTTP";

    public TomcatServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new TomcatServer();
    }
}
