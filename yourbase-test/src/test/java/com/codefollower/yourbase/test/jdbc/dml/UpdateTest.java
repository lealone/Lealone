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
package com.codefollower.yourbase.test.jdbc.dml;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.codefollower.yourbase.test.jdbc.TestBase;

public class UpdateTest extends TestBase {
    @Test
    public void run() throws Exception {
        createTableIfNotExists("UpdateTest");
        testInsert();
        testUpdate();
    }

    void testInsert() throws Exception {
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('01', 'a1', 'b', 51)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('02', 'a1', 'b', 61)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('03', 'a1', 'b', 61)");

        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('25', 'a2', 'b', 51)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('26', 'a2', 'b', 61)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('27', 'a2', 'b', 61)");

        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('50', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('51', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('52', 'a1', 'b', 12)");

        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('75', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('76', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO UpdateTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testUpdate() throws Exception {
        sql = "UPDATE UpdateTest SET f1 = 'a1', cf2.f3 = 61 WHERE _rowkey_= '01'";
        assertEquals(1, stmt.executeUpdate(sql));

        sql = "SELECT f1, cf1.f2, cf2.f3 FROM UpdateTest WHERE _rowkey_ = '01'";
        assertEquals("a1", getStringValue(1));
        assertEquals("b", getStringValue(2));
        assertEquals(61, getIntValue(3, true));
    }
}
