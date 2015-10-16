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
package org.lealone.test.sql.index;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class MainIndexColumnTest extends SqlTestBase {

    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS MainIndexColumnTest CASCADE");
        executeUpdate("create table IF NOT EXISTS MainIndexColumnTest(id int not null, name varchar(50))");

        executeUpdate("CREATE PRIMARY KEY IF NOT EXISTS MainIndexColumnTest_id ON MainIndexColumnTest(id)");

        executeUpdate("insert into MainIndexColumnTest(id, name) values(10, 'a1')");
        executeUpdate("insert into MainIndexColumnTest(id, name) values(20, 'b1')");
        executeUpdate("insert into MainIndexColumnTest(id, name) values(30, 'a2')");

        sql = "select * from MainIndexColumnTest";
        printResultSet();
    }

}
