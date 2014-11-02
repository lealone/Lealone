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
package org.lealone.test.jdbc.misc;

import org.junit.Test;
import org.lealone.test.jdbc.TestBase;

public class ViewTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        createView();
    }

    void init() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS ViewTest("
                + "COLUMN FAMILY cf (id int, name varchar(500), b boolean))");
        insert();
    }

    void insert() throws Exception {
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(1, 1, 'a1', true)");
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(2, 1, 'b1', true)");
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(3, 2, 'a2', false)");
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(4, 2, 'b2', true)");
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(5, 3, 'a3', false)");
        stmt.executeUpdate("insert into ViewTest(_rowkey_, id, name, b) values(6, 3, 'b3', true)");
    }

    void createView() throws Exception {
        sql = "CREATE OR REPLACE FORCE VIEW IF NOT EXISTS my_view COMMENT IS 'my view'(f1,f2) " //
                + "AS SELECT id,name FROM ViewTest";

        stmt.executeUpdate(sql);

        sql = "SELECT * FROM my_view";
        //sql = "SELECT * FROM ViewTest";

        sql = "SELECT * FROM my_view where f1>=2";
        printResultSet();
    }

}
