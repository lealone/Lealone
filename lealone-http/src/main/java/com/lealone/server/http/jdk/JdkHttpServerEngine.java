/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http.jdk;

import com.lealone.server.ProtocolServer;
import com.lealone.server.http.HttpServerEngine;

public class JdkHttpServerEngine extends HttpServerEngine {

    public JdkHttpServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new JdkHttpServer();
    }
}
