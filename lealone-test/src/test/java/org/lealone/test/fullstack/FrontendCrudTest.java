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

import org.lealone.test.SqlScript;
import org.lealone.test.UnitTestBase;
import org.lealone.vertx.LealoneHttpServer;

public class FrontendCrudTest extends UnitTestBase {

    public static void main(String[] args) {
        new FrontendCrudTest().runTest(true, false);
    }

    @Override
    public void test() {
        // 创建user表
        SqlScript.createUserTable(this);
        // 启动HttpServer
        // 在浏览器中打开下面这个URL，测试在前端直接执行crud，在console里面看结果:
        // http://localhost:8080/crud.html
        LealoneHttpServer.start(8080, "./src/test/resources/webroot/", "/api/*");
    }

}
