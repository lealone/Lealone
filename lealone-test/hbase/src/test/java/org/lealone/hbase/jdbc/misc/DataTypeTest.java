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
package org.lealone.hbase.jdbc.misc;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;
import org.lealone.hbase.jdbc.TestBase;

public class DataTypeTest extends TestBase {
    @Test
    public void run() throws Exception {
        createTableSQL("CREATE TABLE IF NOT EXISTS DataTypeTest (f1 int primary key, f2 TINYINT, age tinyint)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS DataTypeTest_idx ON DataTypeTest(age)");

        stmt.executeUpdate("INSERT INTO DataTypeTest(f1, f2, age) VALUES(1, 2, 4)");

        sql = "SELECT * FROM DataTypeTest";
        rs = stmt.executeQuery(sql);
        rs.next();

        assertEquals(2, rs.getByte(2));
        rs.close();

        sql = "SELECT f1, f2, age FROM DataTypeTest WHERE age=4";
        rs = stmt.executeQuery(sql);
        rs.next();

        assertEquals(4, rs.getByte(3));
        rs.close();
    }
}
