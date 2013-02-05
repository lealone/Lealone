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
package com.codefollower.yourbase.test.jdbc.dynamic;

import static junit.framework.Assert.assertEquals;

import java.sql.DriverManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codefollower.yourbase.test.jdbc.TestBase;

public class DynamicConnectionTest extends TestBase {
    //dynamic后面接zookeeper的地址列表
    protected static String url = "jdbc:yourbase:dynamic://127.0.0.1:2181/hbasedb";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conn = DriverManager.getConnection(url, "sa", "");
        stmt = conn.createStatement();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (stmt != null)
            stmt.close();
        if (conn != null)
            conn.close();
    }

    @Test
    public void run() throws Exception {
        String tableName = "DynamicConnectionTest";

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (f1 int primary key, f2 long)");
        stmt.executeUpdate("INSERT INTO " + tableName + "(f1, f2) VALUES(1, 2)");
        stmt.executeUpdate("INSERT INTO " + tableName + "(f1, f2) VALUES(2, 3)");
        stmt.executeUpdate("INSERT INTO " + tableName + "(f1, f2) VALUES(3, 4)");
        sql = "SELECT count(*) FROM " + tableName;
        assertEquals(3, getIntValue(1, true));

    }
}
