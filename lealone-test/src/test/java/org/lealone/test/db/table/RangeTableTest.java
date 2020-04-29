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
package org.lealone.test.db.table;

import org.junit.Test;
import org.lealone.db.result.Result;
import org.lealone.db.table.RangeTable;
import org.lealone.test.db.DbObjectTestBase;

public class RangeTableTest extends DbObjectTestBase {
    @Test
    public void run() {
        String name = RangeTable.NAME;
        Result rs = executeQuery("select count(*) from " + name + "(1,10)");
        assertTrue(rs.next());
        assertEquals(10, getInt(rs, 1));
        rs.close();

        rs = executeQuery("select count(*) from " + name + "(1,10,2)");
        assertTrue(rs.next());
        assertEquals(5, getInt(rs, 1));
        rs.close();

        rs = executeQuery("select * from " + name + "(1,10,2)");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(3, getInt(rs, 1));
        rs.close();
    }
}
