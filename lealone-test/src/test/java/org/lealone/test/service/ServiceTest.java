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
import org.lealone.vertx.LealoneHttpServer;

public class ServiceTest extends UnitTestBase {

    public static final String packageName = ServiceTest.class.getPackage().getName();

    public static void main(String[] args) {
        new ServiceTest().runTest();
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

    // http://localhost:8080/index.html
    private void testFrontendRpcServices() {
        LealoneHttpServer.start(8080, "./src/test/resources/webroot/", "/api/*");
    }

}
