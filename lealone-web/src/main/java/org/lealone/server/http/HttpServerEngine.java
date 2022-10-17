/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.http;

import org.lealone.server.ProtocolServerEngineBase;
import org.lealone.server.vertx.VertxServerEngine;

public abstract class HttpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = VertxServerEngine.NAME;

    public HttpServerEngine(String name) {
        super(name);
    }

}
