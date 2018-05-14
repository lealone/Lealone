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
package org.lealone.test;

import org.lealone.test.service.impl.HelloWorldServiceImpl;
import org.lealone.test.service.impl.UserServiceImpl;

public class SqlScript {

    public static void main(String[] args) {
        new SqlScriptTest().runTest();
    }

    public static interface SqlExecuter {
        void execute(String sql);
    }

    private static class SqlScriptTest extends UnitTestBase {
        @Override
        public void test() {
            createCustomerTable(this);
            createUserTable(this);
            createAllTypeTable(this);
            createUserService(this);
            createHelloWorldService(this);
        }
    }

    private static final String MODEL_PACKAGE_NAME = "org.lealone.test.generated.model";
    private static final String SERVICE_PACKAGE_NAME = "org.lealone.test.generated.service";
    private static final String GENERATED_CODE_PATH = "./src/test/java";

    public static void createCustomerTable(SqlExecuter executer) {
        System.out.println("create table: customer");

        executer.execute("create table customer(id long, name char(10), notes varchar, phone int)" //
                + " package '" + MODEL_PACKAGE_NAME + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'" // 生成领域模型类和查询器类的代码
        );
    }

    public static void createUserTable(SqlExecuter executer) {
        System.out.println("create table: user");

        // 创建表: user
        executer.execute("create table user(name char(10) primary key, notes varchar, phone int, id long)" //
                + " package '" + MODEL_PACKAGE_NAME + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'");
    }

    public static void createAllTypeTable(SqlExecuter executer) {
        // 21种类型，目前不支持GEOMETRY类型
        // INT
        // BOOLEAN
        // TINYINT
        // SMALLINT
        // BIGINT
        // IDENTITY
        // DECIMAL
        // DOUBLE
        // REAL
        // TIME
        // DATE
        // TIMESTAMP
        // BINARY
        // OTHER
        // VARCHAR
        // VARCHAR_IGNORECASE
        // CHAR
        // BLOB
        // CLOB
        // UUID
        // ARRAY
        executer.execute("CREATE TABLE all_type (" //
                + " f1  INT," //
                + " f2  BOOLEAN," //
                + " f3  TINYINT," //
                + " f4  SMALLINT," //
                + " f5  BIGINT," //
                + " f6  IDENTITY," //
                + " f7  DECIMAL," //
                + " f8  DOUBLE," //
                + " f9  REAL," //
                + " f10 TIME," //
                + " f11 DATE," //
                + " f12 TIMESTAMP," //
                + " f13 BINARY," //
                + " f14 OTHER," //
                + " f15 VARCHAR," //
                + " f16 VARCHAR_IGNORECASE," //
                + " f17 CHAR," //
                + " f18 BLOB," //
                + " f19 CLOB," //
                + " f20 UUID," //
                + " f21 ARRAY" //
                + ")" //
                + " PACKAGE '" + MODEL_PACKAGE_NAME + "'" //
                + " GENERATE CODE '" + GENERATED_CODE_PATH + "'");

        System.out.println("create table: all_type");
    }

    public static void createUserService(SqlExecuter executer) {
        System.out.println("create service: user_service");

        // 创建服务: user_service
        executer.execute("create service if not exists user_service (" //
                + " add(user user) long," // 第一个user是参数名，第二个user是参数类型
                + " find(name varchar) user," //
                + " update(user user) int," //
                + " delete(name varchar) int)" //
                + " package '" + SERVICE_PACKAGE_NAME + "'" //
                // 如果是内部类，不能用getClassName()，会包含$字符
                + " implement by '" + UserServiceImpl.class.getCanonicalName() + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'");
    }

    public static void createHelloWorldService(SqlExecuter executer) {
        System.out.println("create service: hello_world_service");

        // 创建服务: hello_world_service
        executer.execute("create service hello_world_service (" //
                + "             say_hello() void," //
                + "             say_goodbye_to(name varchar) varchar" //
                + "         ) package '" + SERVICE_PACKAGE_NAME + "'" //
                + "           implement by '" + HelloWorldServiceImpl.class.getName() + "'" //
                + "           generate code '" + GENERATED_CODE_PATH + "'");
    }
}
