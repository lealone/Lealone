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
package com.codefollower.lealone.test.jdbc.dml;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

public class DeleteTest extends TestBase {
    @Test
    public void run() throws Exception {
        createTableIfNotExists("DeleteTest");
        testInsert();
        testDelete();
    }

    void testInsert() throws Exception {
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('01', 'a1', 'b', 51)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('02', 'a1', 'b', 61)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('03', 'a1', 'b', 61)");

        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('25', 'a2', 'b', 51)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('26', 'a2', 'b', 61)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('27', 'a2', 'b', 61)");

        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('50', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('51', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('52', 'a1', 'b', 12)");

        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('75', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('76', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO DeleteTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testDelete() throws Exception {
        sql = "DELETE FROM DeleteTest WHERE _rowkey_= '01'";
        assertEquals(1, stmt.executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE _rowkey_<= '25'";
        assertEquals(3, stmt.executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE _rowkey_> '25' AND _rowkey_< '50'";
        assertEquals(2, stmt.executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE _rowkey_>= '50'";
        assertEquals(6, stmt.executeUpdate(sql));
    }

}
