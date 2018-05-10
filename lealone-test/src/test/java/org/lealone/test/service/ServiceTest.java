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
package org.lealone.test.service;

import org.lealone.test.UnitTestBase;
import org.lealone.vertx.SockJSSocketServiceHandler;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

public class ServiceTest extends UnitTestBase {

    public static final String packageName = ServiceTest.class.getPackage().getName();

    public static void main(String[] args) {
        ServiceTest t = new ServiceTest();
        t.setEmbedded(true);
        t.setInMemory(true);
        t.runTest();
    }

    @Override
    public void test() {
        createTable();
        createServices();
        testBackendRpcServices(); // 在后端执行RPC
        testFrontendRpcServices(); // 在前端执行RPC
    }

    private void createTable() {
        System.out.println("create table: user");
        String packageName = ServiceTest.packageName + ".generated";

        execute("create table user(id long, name char(10), notes varchar, phone int)" //
                + " package '" + packageName + "'" //
                + " generate code './src/test/java'"); // 生成领域模型类和查询器类的代码
    }

    private void createServices() {
        System.out.println("create services");
        ServiceProvider.execute(this);
    }

    private void testBackendRpcServices() {
        System.out.println("test backend rpc services");
        String url = getURL();
        System.out.println("jdbc url: " + url);

        ServiceConsumer.execute(url);
    }

    private void testFrontendRpcServices() {
        startHttpServer();
    }

    // http://localhost:8080/index.html
    private static void startHttpServer() {
        VertxOptions opt = new VertxOptions();
        opt.setBlockedThreadCheckInterval(Integer.MAX_VALUE);
        Vertx vertx = Vertx.vertx(opt);
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        // router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
        setSockJSHandler(vertx, router);
        // 放在最后
        setStaticHandler(vertx, router);

        server.requestHandler(router::accept).listen(8080, res -> {
            if (res.succeeded()) {
                System.out.println("Server is now listening on actual port: " + server.actualPort());
            } else {
                System.out.println("Failed to bind!");
            }
        });
    }

    private static void setStaticHandler(Vertx vertx, Router router) {
        StaticHandler sh = StaticHandler.create("./src/test/resources/webroot/");
        sh.setCachingEnabled(false);
        router.route("/*").handler(sh);
    }

    private static void setSockJSHandler(Vertx vertx, Router router) {
        SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
        sockJSHandler.socketHandler(new SockJSSocketServiceHandler());
        router.route("/api/*").handler(sockJSHandler);
    }

}
