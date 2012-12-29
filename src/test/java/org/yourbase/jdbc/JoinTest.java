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
package org.yourbase.jdbc;

import org.junit.Test;

public class JoinTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        insert();
        test();
    }

    //topTableFilter.getTable().getName().equalsIgnoreCase("JoinTest1")
    void init() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS JoinTest1(" //
                + "COLUMN FAMILY cf(id int, name varchar(500), b boolean))");

        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS JoinTest2(" //
                + "COLUMN FAMILY cf(id2 int, name2 varchar(500)))");

        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS JoinTest3(" //
                + "COLUMN FAMILY cf(id3 int, name3 varchar(500)))");

        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS JoinTest4(" //
                + "COLUMN FAMILY cf(id int, name varchar(500)))");
    }

    void insert() throws Exception {
        stmt.executeUpdate("insert into JoinTest1(_rowkey_, id, name, b) values(1, 10, 'a1', true)");
        stmt.executeUpdate("insert into JoinTest1(_rowkey_, id, name, b) values(2, 20, 'b1', true)");
        stmt.executeUpdate("insert into JoinTest1(_rowkey_, id, name, b) values(3, 30, 'a2', false)");
        stmt.executeUpdate("insert into JoinTest1(_rowkey_, id, name, b) values(4, 40, 'b2', true)");

        stmt.executeUpdate("insert into JoinTest2(_rowkey_, id2, name2) values(1, 60, 'a11')");
        stmt.executeUpdate("insert into JoinTest2(_rowkey_, id2, name2) values(2, 70, 'a11')");
        stmt.executeUpdate("insert into JoinTest2(_rowkey_, id2, name2) values(3, 80, 'a11')");
        stmt.executeUpdate("insert into JoinTest2(_rowkey_, id2, name2) values(4, 90, 'a11')");

        stmt.executeUpdate("insert into JoinTest3(_rowkey_, id3, name3) values(1, 100, 'a11')");
        stmt.executeUpdate("insert into JoinTest3(_rowkey_, id3, name3) values(2, 200, 'a11')");

        stmt.executeUpdate("insert into JoinTest4(_rowkey_, id, name) values(1, 10, 'a1')");
        stmt.executeUpdate("insert into JoinTest4(_rowkey_, id, name) values(2, 10, 'a1')");
        stmt.executeUpdate("insert into JoinTest4(_rowkey_, id, name) values(3, 20, 'a1')");
        stmt.executeUpdate("insert into JoinTest4(_rowkey_, id, name) values(4, 30, 'a1')");
    }

    void test() throws Exception {
        sql = "select rownum, * from JoinTest1 LEFT OUTER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 RIGHT OUTER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 INNER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 CROSS JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 NATURAL JOIN JoinTest2";

        sql = "select rownum, * from JoinTest1 LEFT OUTER JOIN JoinTest3 NATURAL JOIN JoinTest2";
        sql = "FROM USER() SELECT * ";

        sql = "SELECT * FROM (JoinTest1)";
        sql = "SELECT * FROM (JoinTest1 LEFT OUTER JOIN (JoinTest2))";

        sql = "SELECT * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 LEFT OUTER JOIN JoinTest3";

        sql = "SELECT t1.id, t1.b FROM JoinTest1 t1 NATURAL JOIN JoinTest4 t2";

        //org.h2.table.TableFilter.next()
        //打断点:table.getName().equalsIgnoreCase("JoinTest1") || table.getName().equalsIgnoreCase("JoinTest2")
        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON id>30";
        sql = "SELECT rownum, * FROM JoinTest1 RIGHT OUTER JOIN JoinTest2 ON id2>70";
        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 JOIN JoinTest3";

        sql = "SELECT rownum, * FROM (JoinTest1) LEFT OUTER JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN (JoinTest2) ON id>30";
        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON id>30 WHERE 1>2";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON name2=null";

        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 WHERE JoinTest1._rowkey_ = 1 and JoinTest2._rowkey_ = 2";

        sql = "SELECT rownum, id, id2 FROM JoinTest1 JOIN JoinTest2 WHERE JoinTest1._rowkey_ = 1 and JoinTest2._rowkey_ = 2";

        //sql = "SELECT rownum, * FROM JoinTest1";
        printResultSet();

    }
}
