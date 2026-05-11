/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http;

import com.lealone.server.ProtocolServerEngineBase;

public abstract class HttpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "HTTP";
    public static final int DEFAULT_PORT = 8080;

    public HttpServerEngine(String name) {
        super(name);
    }
}
