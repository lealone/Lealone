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

import org.lealone.test.UnitTestBase;
import org.lealone.test.fullstack.generated.User;
import org.lealone.test.fullstack.generated.UserService;
import org.lealone.vertx.LealoneHttpServer;

public class FullStackTest extends UnitTestBase {

    public static void main(String[] args) {
        new FullStackTest().runTest(true, false);
    }

    @Override
    public void test() {
        init();

        // 从后端调用服务
        callService();

        // 启动HttpServer
        // 在浏览器中打开下面这个URL，测试从前端发起服务调用，在console里面看结果:
        // http://localhost:8080/FullStackTest.html
        LealoneHttpServer.start(8080, "./src/test/resources/webroot/", "/api/*");
    }

    void setJdbcUrl() {
        String url = getURL();
        System.setProperty("lealone.jdbc.url", url);
        System.out.println("jdbc url: " + url);
    }

    void init() {
        setJdbcUrl();
        String packageName = FullStackTest.class.getPackage().getName();

        // 创建表: user
        execute("create table user(name char(10) primary key, notes varchar, phone int)" //
                + " package '" + packageName + ".generated'" //
                + " generate code './src/test/java'");

        System.out.println("create table: user");

        // 创建服务: user_service
        execute("create service if not exists user_service (" //
                + " add(user user) long," // 第一个user是参数名，第二个user是参数类型
                + " find(name varchar) user," //
                + " update(user user) int," //
                + " delete(name varchar) int)" //
                + " package '" + packageName + ".generated'" //
                + " implement by '" + UserServiceImpl.class.getCanonicalName() + "'" // 不能用getClassName()，会包含$字符
                + " generate code './src/test/java'");

        System.out.println("create service: user_service");
    }

    void callService() {
        String url = getURL();
        UserService userService = UserService.create(url);

        User user = new User().name.set("zhh").phone.set(123);
        userService.add(user);

        user = userService.find("zhh");

        user.notes.set("call remote service");
        userService.update(user);

        userService.delete("zhh");
    }

    public static class UserServiceImpl implements UserService {
        @Override
        public Long add(User user) {
            return user.insert();
        }

        @Override
        public User find(String name) {
            return new User().where().name.eq(name).findOne();
        }

        @Override
        public Integer update(User user) {
            return user.where().name.eq(user.name.get()).update();
        }

        @Override
        public Integer delete(String name) {
            return new User().where().name.eq(name).delete();
        }
    }

}
