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
package org.lealone.test.service.demo;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class Provider extends SqlTestBase {

    private static final String SERVICE_PACKAGE_NAME = Provider.class.getPackage().getName() + ".generated";
    private static final String GENERATED_CODE_PATH = "./src/test/java";

    @Test
    public void createService() {
        // 创建服务: demo_service
        executeUpdate("create service if not exists demo_service (" //
                + "        say_hello(name varchar) varchar" //
                + "    )" //
                + "    package '" + SERVICE_PACKAGE_NAME + "'" //
                + "    implement by '" + DemoServiceImpl.class.getName() + "'" //
                + "    generate code '" + GENERATED_CODE_PATH + "'");
    }
}