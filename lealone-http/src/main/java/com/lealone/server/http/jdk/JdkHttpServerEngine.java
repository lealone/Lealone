/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http.jdk;

import com.lealone.server.ProtocolServer;
import com.lealone.server.ProtocolServerEngineBase;

public class JdkHttpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "JDKHTTP";
    public static final int DEFAULT_PORT = 8080;

    public JdkHttpServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new JdkHttpServer();
    }
}
