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
package com.codefollower.lealone.test.jdbc.index;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

public class IndexTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        insert();
        select();
    }

    void init() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS IndexTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS IndexTest (f1 int NOT NULL, f2 int, f3 varchar)");
        stmt.executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS idx0 ON IndexTest(f1)");
        stmt.executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS idx1 ON IndexTest(f2)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx2 ON IndexTest(f3, f2)");

        //stmt.executeUpdate("ALTER INDEX idx0 RENAME TO idx2");

    }

    void insert() throws Exception {
        stmt.executeUpdate("DELETE FROM IndexTest");
        stmt.executeUpdate("INSERT INTO IndexTest(f1, f2, f3) VALUES(300, 30, 'a')");
        stmt.executeUpdate("INSERT INTO IndexTest(f1, f2, f3) VALUES(100, 10, 'b')");
        stmt.executeUpdate("INSERT INTO IndexTest(f1, f2, f3) VALUES(200, 20, 'c')");
        try {
            stmt.executeUpdate("INSERT INTO IndexTest(f1, f2, f3) VALUES(200, 20, 'c')");
            Assert.fail("insert duplicate key: 20");
        } catch (SQLException e) {
            //e.printStackTrace();
        }
    }

    void select() throws Exception {
        sql = "SELECT f1, f2, f3 FROM IndexTest";
        printResultSet();

        sql = "SELECT f1, f2, f3 FROM IndexTest WHERE f1 >= 200";
        printResultSet();

        sql = "SELECT count(*) FROM IndexTest WHERE f1 >= 200";
        assertEquals(2, getIntValue(1, true));

        sql = "SELECT f1, f2, f3 FROM IndexTest WHERE f2 >= 20";
        printResultSet();

        sql = "SELECT count(*) FROM IndexTest WHERE f2 >= 20";
        assertEquals(2, getIntValue(1, true));

        sql = "SELECT f1, f2, f3 FROM IndexTest WHERE f3 >= 'b' AND f3 <= 'c'";
        printResultSet();

        sql = "SELECT count(*) FROM IndexTest WHERE f3 >= 'b' AND f3 <= 'c'";
        assertEquals(2, getIntValue(1, true));
    }
}
