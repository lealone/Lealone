/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.vertx;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

public class LealoneHttpServer {

    private static final Logger logger = LoggerFactory.getLogger(LealoneHttpServer.class);

    public static void start(int port, String webRoot) {
        start(port, webRoot, null);
    }

    public static void start(int port, String webRoot, String apiPath) {
        if (apiPath == null)
            apiPath = "/_lealone_sockjs_/*";
        final String path = apiPath;
        VertxOptions opt = new VertxOptions();
        opt.setBlockedThreadCheckInterval(Integer.MAX_VALUE);
        Vertx vertx = Vertx.vertx(opt);
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        String syncRequestUrl = "/_lealone_sync_request_";
        router.post(syncRequestUrl).handler(BodyHandler.create());
        router.post(syncRequestUrl).handler(routingContext -> {
            String command = routingContext.request().params().get("command");
            Buffer result = LealoneServiceHandler.handle(routingContext, command);
            routingContext.request().response().headers().set("Access-Control-Allow-Origin", "*");
            routingContext.request().response().end(result);
        });

        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
        setSockJSHandler(vertx, router, apiPath);
        // 放在最后
        setStaticHandler(vertx, router, webRoot);

        server.requestHandler(router::accept).listen(port, res -> {
            if (res.succeeded()) {
                logger.info("web root: " + webRoot);
                logger.info("sockjs path: " + path);
                logger.info("http server is now listening on port: " + server.actualPort());
            } else {
                logger.error("failed to bind " + port + " port!", res.cause());
            }
        });
    }

    private static void setStaticHandler(Vertx vertx, Router router, String webRoot) {
        for (String root : webRoot.split(",", -1)) {
            root = root.trim();
            if (root.isEmpty())
                continue;
            StaticHandler sh = StaticHandler.create(root);
            sh.setCachingEnabled(false);
            router.route("/*").handler(sh);
        }
    }

    private static void setSockJSHandler(Vertx vertx, Router router, String apiPath) {
        SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
        sockJSHandler.socketHandler(new LealoneServiceHandler());
        router.route(apiPath).handler(sockJSHandler);
    }

}
