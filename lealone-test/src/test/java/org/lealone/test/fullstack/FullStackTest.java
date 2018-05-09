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
package org.lealone.test.fullstack;

import org.lealone.test.TestBase;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class FullStackTest extends TestBase {

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        // System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, "true");

        // TestBase.initTransactionEngine();
        FullStackTest t = new FullStackTest();
        t.setEmbedded(true).setInMemory(true);
        // t.printURL();
        t.run();
    }

    private void run() {
        startHttpServer();
    }

    private void startHttpServer() {
        final long s1 = System.currentTimeMillis();
        VertxOptions opt = new VertxOptions();
        opt.setBlockedThreadCheckInterval(Integer.MAX_VALUE);
        Vertx vertx = Vertx.vertx(opt);
        long s2 = System.currentTimeMillis();
        System.out.println("Total time init vertx: " + (s2 - s1) + "ms");

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
        StaticHandler sh = StaticHandler.create().setWebRoot("./src/test/resources/webroot");
        sh.setCachingEnabled(false);
        router.route("/static/*").handler(sh);

        // setSockJSHandler(vertx, router);

        server.requestHandler(router::accept).listen(8080, rs -> {
            if (rs.succeeded()) {
                System.out.println("SockJS listen 8000");
                long s22 = System.currentTimeMillis();
                System.out.println("Total time: " + (s22 - s1) + "ms");
            }
        });
    }

}
