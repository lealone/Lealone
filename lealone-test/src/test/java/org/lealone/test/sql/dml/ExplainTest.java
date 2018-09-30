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
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExplainTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS ExplainTest");
        executeUpdate("create table IF NOT EXISTS ExplainTest(id int, name varchar(500), b boolean)");
        executeUpdate("CREATE INDEX IF NOT EXISTS ExplainTestIndex ON ExplainTest(name)");

        executeUpdate("insert into ExplainTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into ExplainTest(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into ExplainTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into ExplainTest(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into ExplainTest(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into ExplainTest(id, name, b) values(3, 'b3', true)");

        sql = "delete top 3 from ExplainTest";
        sql = "delete top 3 from ExplainTest where name='a1'";
        sql = "delete top 3 from ExplainTest where 'a1'>name";
        sql = "delete top 3 from ExplainTest where name = null";
        sql = "delete top 3 from ExplainTest where name != null";
        sql = "delete top 3 from ExplainTest where name > null";

        sql = "delete from ExplainTest where name > 'b1'";
        sql = "delete from ExplainTest where id>2";
        sql = "delete from ExplainTest where 3<2";
        sql = "delete from ExplainTest where b";
        sql = "delete from ExplainTest where 3>2";

        sql = "EXPLAIN ANALYZE " + sql;
        printResultSet();

        sql = "select * from ExplainTest where name > 'b1'";
        sql = "select count(name) from ExplainTest where name > 'b1' group by name";
        sql = "select name, count(name) from ExplainTest group by name";

        sql = "WITH RECURSIVE myTempViewName(f1,f2,f3) "
                + "AS(select * from ExplainTest UNION ALL select * from ExplainTest) select f1, f2 from myTempViewName";
        sql = "EXPLAIN " + sql;
        printResultSet();
    }
}
