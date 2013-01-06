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
package com.codefollower.yourbase.test.jdbc;

import static junit.framework.Assert.assertEquals;
import java.sql.PreparedStatement;
import org.junit.Test;

public class PreparedStatementTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        test();
    }

    void init() throws Exception {
        createTableSQL("CREATE HBASE TABLE IF NOT EXISTS PreparedStatementTest(COLUMN FAMILY cf)");

        stmt.executeUpdate("INSERT INTO PreparedStatementTest(_rowkey_, f1, f2) VALUES('01', 'a1', 10)");
        stmt.executeUpdate("INSERT INTO PreparedStatementTest(_rowkey_, f1, f2) VALUES('02', 'a2', 50)");
        stmt.executeUpdate("INSERT INTO PreparedStatementTest(_rowkey_, f1, f2) VALUES('03', 'a3', 30)");
    }

    void test() throws Exception {
        sql = "SELECT count(*) FROM PreparedStatementTest WHERE f2 >= ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 30);
        rs = ps.executeQuery();
        rs.next();
        assertEquals(2, getIntValue(1, true));
        ps.close();
    }
}
