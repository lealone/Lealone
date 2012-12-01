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

import static junit.framework.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

//TODO 由于HBase不支持事务，目前只能按rowKey删除或更新单条记录
public class JDBCTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        testInsert();
        testUpdate();
        testDelete();
        testSelect();
    }

    void init() throws Exception {
        //建立了4个分区
        //------------------------
        //分区1: rowKey < 25
        //分区2: 25 <= rowKey < 50
        //分区3: 50 <= rowKey < 75
        //分区4: rowKey > 75
        createTable("JDBCTest", "25", "50", "75");
    }

    void testInsert() throws Exception {
        //f1没有加列族前缀，默认是cf1，按CREATE HBASE TABLE中的定义顺序，哪个在先默认就是哪个
        //或者在表OPTIONS中指定DEFAULT_COLUMN_FAMILY_NAME参数
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('01', 'a', 'b', 51)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('02', 'a1', 'b', 61)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('03', 'a1', 'b', 61)");

        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('05', 'a2', 'b', 51)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('06', 'a2', 'b', 61)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('07', 'a2', 'b', 61)");

        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('10', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('11', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO JDBCTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('12', 'a1', 'b', 12)");

        //TODO H2数据库会默认把标识符转成大写，这个问题未解决，所以这里表名、列族名用大写
        HTable t = new HTable(HBaseConfiguration.create(), "JDBCTEST");
        byte[] cf1 = Bytes.toBytes("CF1");
        byte[] cf2 = Bytes.toBytes("CF2");
        Get get = new Get(Bytes.toBytes("10"));
        Result result = t.get(get);
        assertEquals("a1", toS(result.getValue(cf1, Bytes.toBytes("F1"))));
        assertEquals("b", toS(result.getValue(cf1, Bytes.toBytes("F2"))));
        assertEquals("12", toS(result.getValue(cf2, Bytes.toBytes("F3"))));
    }

    void testUpdate() throws Exception {
        sql = "UPDATE JDBCTest SET f1 = 'a1', cf2.f3 = 61 WHERE _rowkey_= '01'";
        assertEquals(1, stmt.executeUpdate(sql));

        sql = "SELECT f1, cf1.f2, cf2.f3 FROM JDBCTest WHERE _rowkey_ = '01'";
        assertEquals("a1", getStringValue(1));
        assertEquals("b", getStringValue(2));
        assertEquals(61, getIntValue(3, true));
    }

    void testDelete() throws Exception {
        sql = "SELECT count(*) FROM JDBCTest";
        assertEquals(9, getIntValue(1, true));

        sql = "DELETE FROM JDBCTest WHERE _rowkey_= '12'";
        assertEquals(1, stmt.executeUpdate(sql));

        sql = "SELECT count(*) FROM JDBCTest";
        assertEquals(8, getIntValue(1, true));
    }

    void testSelect() throws Exception {
        sql = "select _rowkey_, f1, f2, cf2.f3 from JDBCTest";
        printResultSet();

        where();
        orderBy();
        groupBy();
    }

    private void where() throws Exception {
        sql = "SELECT count(*) FROM JDBCTest WHERE f1 = 'a2'";
        assertEquals(4, getIntValue(1, true));

        sql = "SELECT count(*) FROM JDBCTest WHERE _rowkey_ >= '10' AND f1 = 'a2'";
        assertEquals(1, getIntValue(1, true));

        sql = "SELECT count(*) FROM JDBCTest WHERE _rowkey_ = '12' AND f1 = 'a2'";
        assertEquals(0, getIntValue(1, true));
    }

    private void orderBy() throws Exception {
        sql = "FROM JDBCTest SELECT f1, f2, cf2.f3 ORDER BY f1 desc";
        printResultSet();
    }

    private void groupBy() throws Exception {
        sql = "SELECT f1, count(f1) FROM JDBCTest GROUP BY f1";
        sql = "SELECT f1, count(f1) FROM JDBCTest GROUP BY f1 HAVING f1 >= 'a1'";
        printResultSet();
    }

}
