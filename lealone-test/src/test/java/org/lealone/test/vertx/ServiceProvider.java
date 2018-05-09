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
package org.lealone.test.vertx;

import java.sql.SQLException;
import java.sql.Statement;

public class ServiceProvider {

    public static void execute(Statement stmt) throws SQLException {
        // 创建服务: user_service
        stmt.executeUpdate("create service if not exists user_service (" //
                + "             add(user user) user," // 第一个user是参数名，第二个user是参数类型，第三个user是返回值类型
                + "             find(id long) user," //
                + "             update(user user) boolean," //
                + "             delete(id long) boolean," //
                + "         ) package 'org.lealone.test.vertx.generated'" //
                + "           implement by 'org.lealone.test.vertx.impl.UserServiceImpl'");

        // 创建服务: hello_service
        stmt.executeUpdate("create service hello_world_service (" //
                + "             say_hello() void" //
                + "         ) package 'org.lealone.test.vertx.generated'" //
                + "           implement by 'org.lealone.test.vertx.impl.HelloServiceImpl'");
    }

}
