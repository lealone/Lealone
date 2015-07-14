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

import java.sql.ResultSet;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class SelectTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        createTable("SelectTest");
        testInsert();
        testSelect();
        testAggregate();
    }

    void testInsert() throws Exception {
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('01', 'a1', 'a', 51)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('03', 'a1', 'c', 61)");

        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('25', 'a2', 'd', 51)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('26', 'a2', 'e', 61)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('27', 'a2', 'f', 61)");

        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('50', 'a1', 'g', 12)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('51', 'a2', 'h', 12)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('52', 'a1', 'i', 12)");

        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('75', 'a1', 'j', 12)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('76', 'a2', 'k', 12)");
        executeUpdate("INSERT INTO SelectTest(pk, f1, f2, f3) VALUES('77', 'a1', 'l', 12)");
    }

    void testSelect() throws Exception {
        sql = "SELECT count(*) FROM SelectTest";
        assertEquals(12, getIntValue(1, true));

        sql = "SELECT pk, f1, f2, f3 from SelectTest";
        printResultSet();

        sql = "SELECT pk FROM SelectTest";
        printResultSet();

        sql = "SELECT pk FROM SelectTest WHERE f3 = 12";
        printResultSet();

        sql = "SELECT count(*) FROM SelectTest WHERE f3 = 12";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT pk, f1, f2, f3 FROM SelectTest";
        stmt.setFetchSize(2);
        printResultSet();

        where();
        orderBy();
        groupBy();
        limit();

        testAlias();
    }

    private void where() throws Exception {
        sql = "SELECT count(*) FROM SelectTest WHERE f1 = 'a2'";
        assertEquals(5, getIntValue(1, true));

        sql = "SELECT count(*) FROM SelectTest WHERE pk >= '50' AND f1 = 'a2'";
        assertEquals(2, getIntValue(1, true));

        sql = "SELECT count(*) FROM SelectTest WHERE pk = '75' AND f1 = 'a2'";
        assertEquals(0, getIntValue(1, true));
    }

    private void orderBy() throws Exception {
        sql = "FROM SelectTest SELECT f1, f2, f3 ORDER BY f1 desc";
        printResultSet();
    }

    private void groupBy() throws Exception {
        sql = "SELECT f1, count(f1) FROM SelectTest GROUP BY f1";
        sql = "SELECT f1, count(f1) FROM SelectTest GROUP BY f1 HAVING f1 >= 'a1'";
        printResultSet();
    }

    private void limit() throws Exception {
        sql = "SELECT f1, f2, f3 FROM SelectTest ORDER BY f2 desc LIMIT 2 OFFSET 1";
        assertEquals("k", getStringValue(2, true));
        // printResultSet();

        sql = "SELECT f1, f2, f3 FROM SelectTest ORDER BY f2 desc LIMIT 2";
        assertEquals("l", getStringValue(2, true));
        // printResultSet();

        // TODO H2数据库不支持LIMIT和聚合函数一起用，会忽略lIMIT
        sql = "SELECT count(*) FROM SelectTest LIMIT 1";
        // assertEquals(1, getIntValue(1, true));

        sql = "SELECT * FROM SelectTest LIMIT 1";
        // printResultSet();
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
    }

    private void testAlias() throws Exception {
        // 表别名
        sql = "SELECT st.f1 FROM SelectTest st";
        printResultSet();

        // where中出现列别名
        sql = "SELECT pk AS A FROM SelectTest where A='01'";
        printResultSet();

        // having中出现列别名
        sql = "SELECT f3 AS A, COUNT(*) FROM SelectTest GROUP BY A HAVING A>12";
        printResultSet();
    }

    void testAggregate() throws Exception {
        sql = "select sum(f3) from SelectTest";
        assertEquals(418, getIntValue(1, true));
        sql = "select avg(f3) from SelectTest";
        // 因为f3是int，所以内部已进行4舍5入
        assertEquals(34.0, getDoubleValue(1, true), 0.2);
    }
}
