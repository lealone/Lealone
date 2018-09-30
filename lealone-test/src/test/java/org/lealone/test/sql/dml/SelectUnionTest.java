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

public class SelectUnionTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS SelectUnionTest1");
        executeUpdate("CREATE TABLE IF NOT EXISTS SelectUnionTest1(id int, name varchar(500), b boolean, id1 int)");
        executeUpdate("CREATE INDEX IF NOT EXISTS SelectUnionTestIndex1 ON SelectUnionTest1(name)");

        executeUpdate("DROP TABLE IF EXISTS SelectUnionTest2");
        executeUpdate("CREATE TABLE IF NOT EXISTS SelectUnionTest2(id int, name varchar(500), b boolean, id2 int)");
        executeUpdate("CREATE INDEX IF NOT EXISTS SelectUnionTestIndex2 ON SelectUnionTest2(name)");

        executeUpdate("insert into SelectUnionTest1(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into SelectUnionTest1(id, name, b) values(3, 'b3', true)");

        executeUpdate("insert into SelectUnionTest2(id, name, b) values(4, 'a1', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(4, 'b1', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(5, 'a2', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(5, 'b2', true)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(6, 'a3', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(6, 'b3', true)");

        executeUpdate("insert into SelectUnionTest2(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into SelectUnionTest2(id, name, b) values(3, 'b3', true)");

        // 查询列个数必须一样
        sql = "select id from SelectUnionTest1 UNION select name, b from SelectUnionTest2 order by id";

        // 查询列名必须一样
        sql = "select id from SelectUnionTest1 UNION select name from SelectUnionTest2 order by id";

        sql = "select id from SelectUnionTest1 UNION select id from SelectUnionTest2 order by id";

        // 这两条是等价的，没有重复
        sql = "select * from SelectUnionTest1 UNION select * from SelectUnionTest2 order by id";
        sql = "select * from SelectUnionTest1 UNION DISTINCT select * from SelectUnionTest2 order by id";

        // 有重复
        sql = "select * from SelectUnionTest1 UNION ALL select * from SelectUnionTest2 order by id";

        // 这两条是等价的，结果集有重复
        sql = "select * from SelectUnionTest1 EXCEPT select * from SelectUnionTest2 order by id";
        sql = "select * from SelectUnionTest1 MINUS select * from SelectUnionTest2 order by id";

        sql = "select * from SelectUnionTest1 INTERSECT select * from SelectUnionTest2 order by id";

        // sql = "select id, name from SelectUnionTest1 INTERSECT select id, name from SelectUnionTest2 order by id";

        // 排序列必须是左边的select字段列表中的
        // 这两条sql都不对
        // sql = "select id, name from SelectUnionTest1 INTERSECT select id2, name from SelectUnionTest2 order by id2";
        // sql = "select id, name from SelectUnionTest1 INTERSECT select id, name from SelectUnionTest2 order by id2";

        printResultSet();
    }
}
