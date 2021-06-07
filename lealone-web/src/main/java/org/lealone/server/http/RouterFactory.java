/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.http;

import java.util.Map;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public interface RouterFactory {

    Router createRouter(Map<String, String> config, Vertx vertx);

}
