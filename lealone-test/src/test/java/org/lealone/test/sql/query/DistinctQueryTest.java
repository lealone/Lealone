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
package org.lealone.test.sql.query;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DistinctQueryTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS DistinctQueryTest");
        stmt.executeUpdate("create table IF NOT EXISTS DistinctQueryTest(f1 int SELECTIVITY 10, f2 int, f3 int)");
        stmt.executeUpdate("create index IF NOT EXISTS DistinctQueryTest_i1 on DistinctQueryTest(f1)");
        stmt.executeUpdate("create index IF NOT EXISTS DistinctQueryTest_i1_2 on DistinctQueryTest(f1,f2)");

        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(1,3,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(8,2,3)");
        stmt.executeUpdate("insert into DistinctQueryTest(f1, f2, f3) values(5,9,3)");

        sql = "select distinct * from DistinctQueryTest where f1 > 3";
        sql = "select distinct f1 from DistinctQueryTest";
        int count = printResultSet();
        assertEquals(4, count);
        sql = "select count(distinct f1) from DistinctQueryTest";
        assertEquals(4, getIntValue(1, true));

        sql = "select distinct f1, f2 from DistinctQueryTest";
        count = printResultSet();
        assertEquals(6, count);
        // 不支持多个字段
        // sql = "select count(distinct f1, f2) from DistinctQueryTest";
    }
}
