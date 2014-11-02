/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.test.jdbc.ddl;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.lealone.test.jdbc.TestBase;

public class CreateUserDataTypeTest extends TestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP DOMAIN IF EXISTS EMAIL");
        //VALUE是CREATE DOMAIN语句的默认临时列名
        stmt.executeUpdate("CREATE DOMAIN IF NOT EXISTS EMAIL AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");
        stmt.executeUpdate("CREATE TYPE IF NOT EXISTS EMAIL AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");
        stmt.executeUpdate("CREATE DATATYPE IF NOT EXISTS EMAIL AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");

        //stmt.executeUpdate("CREATE DATATYPE IF NOT EXISTS int AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");

        //从第二个名称开始的都是隐藏类型的，如下面的int
        //new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"}
        //隐藏类型在用户在数据库中没有建表时可以覆盖
        //如CREATE DATATYPE IF NOT EXISTS int AS VARCHAR(255)
        //但是非隐藏类型就不能覆盖
        //如CREATE DATATYPE IF NOT EXISTS integer AS VARCHAR(255)
        try {
            stmt.executeUpdate("CREATE DATATYPE IF NOT EXISTS integer AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");
            Assert.fail("not throw SQLException");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("user data type"));
        }
    }
}
