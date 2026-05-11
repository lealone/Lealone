/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.jdk;

import com.lealone.http.HttpServerEngine;
import com.lealone.server.ProtocolServer;

public class JdkHttpServerEngine extends HttpServerEngine {

    public JdkHttpServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new JdkHttpServer();
    }
}
