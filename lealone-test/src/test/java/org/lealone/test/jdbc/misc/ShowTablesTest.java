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
package org.lealone.test.jdbc.misc;

import org.junit.Test;
import org.lealone.test.jdbc.TestBase;

import static junit.framework.Assert.assertTrue;

public class ShowTablesTest extends TestBase {
    @Test
    public void run() throws Exception {
        String tableName = "ShowTablesTest";
        String tableName2 = "ShowTablesTest2";

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ShowTablesTest(id int, name varchar(500))");
        //HBase表
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS ShowTablesTest2(COLUMN FAMILY cf1)");

        sql = "show tables";
        printResultSet();
        assertTrue(contains(tableName));
        assertTrue(contains(tableName2));

        stmt.executeUpdate("DROP TABLE IF EXISTS ShowTablesTest");
        stmt.executeUpdate("DROP TABLE IF EXISTS ShowTablesTest2");

        sql = "show tables";
        printResultSet();
        assertTrue(!contains(tableName));
        assertTrue(!contains(tableName2));
    }

    public boolean contains(String tableName) throws Exception {
        rs = stmt.executeQuery(sql);
        boolean result = false;
        while (rs.next()) {
            if (rs.getString(1).equalsIgnoreCase(tableName)) {
                result = true;
                break;
            }
        }
        rs.close();
        rs = null;
        return result;
    }
}
