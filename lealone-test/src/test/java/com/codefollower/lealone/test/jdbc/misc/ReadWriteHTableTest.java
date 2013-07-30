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
package com.codefollower.lealone.test.jdbc.misc;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

//读写已存在的HTable
public class ReadWriteHTableTest extends TestBase {
    protected static final Configuration conf = HBaseConfiguration.create();
    protected static final Properties prop = new Properties();

    private String tableName = "ReadWriteHTableTest".toUpperCase();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        prop.setProperty("user", "sa");
        prop.setProperty("password", "");
        //prop.setProperty("DATABASE_TO_UPPER", "false");
        conn = DriverManager.getConnection(getURL(), prop);
        stmt = conn.createStatement();
    }

    @Test
    public void run() throws Exception {
        createHTable("CF1", "CF2");
        putRows();

        createSQLTable();
        testInsert();
        testSelect();
    }

    void createSQLTable() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS " + tableName + "(" //
                + "COLUMN FAMILY cf1(f1 varchar(50)), COLUMN FAMILY CF2(F2 varchar(50)))");
    }

    void testInsert() throws Exception {
        stmt.executeUpdate("INSERT INTO " + tableName + "(_rowkey_, cf1.f1, cf2.f2) VALUES('50', 'a', 'b')");
        stmt.executeUpdate("INSERT INTO " + tableName + "(_rowkey_, cf1.f1, cf2.f2) VALUES('60', 'a', 'b')");
        stmt.executeUpdate("INSERT INTO " + tableName + "(_rowkey_, cf1.f1, cf2.f2) VALUES('70', 'a', 'b')");
    }

    void testSelect() throws Exception {
        sql = "SELECT _rowkey_, cf1.f1, cf2.f2 FROM " + tableName;
        printResultSet();

        sql = "SELECT count(*) FROM " + tableName;
        assertEquals(5, getIntValue(1, true));
    }

    void createHTable(String... familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            htd.addFamily(hcd);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
    }

    void putRows() throws IOException {
        HTable t = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes("10"));
        put.add(Bytes.toBytes("CF1"), Bytes.toBytes("F1"), 2, Bytes.toBytes("v1"));
        put.add(Bytes.toBytes("CF2"), Bytes.toBytes("F2"), 2, Bytes.toBytes("v2"));
        t.put(put);

        put = new Put(Bytes.toBytes("20"));
        put.add(Bytes.toBytes("CF1"), Bytes.toBytes("F1"), 2, Bytes.toBytes("v1"));
        put.add(Bytes.toBytes("CF2"), Bytes.toBytes("F2"), 2, Bytes.toBytes("v2"));
        t.put(put);
    }
}
