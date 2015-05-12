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
package org.lealone.test.sql.misc;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ViewTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        createView();
    }

    void init() throws Exception {
        executeUpdate("CREATE TABLE IF NOT EXISTS ViewTest(id int, name varchar(500), b boolean)");
        insert();
    }

    void insert() throws Exception {
        executeUpdate("insert into ViewTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into ViewTest(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into ViewTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into ViewTest(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into ViewTest(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into ViewTest(id, name, b) values(3, 'b3', true)");
    }

    void createView() throws Exception {
        sql = "CREATE OR REPLACE FORCE VIEW IF NOT EXISTS my_view COMMENT IS 'my view'(f1,f2) " //
                + "AS SELECT id,name FROM ViewTest";

        executeUpdate(sql);

        sql = "SELECT * FROM my_view";
        //sql = "SELECT * FROM ViewTest";

        sql = "SELECT * FROM my_view where f1>=2";
        printResultSet();
    }

}
