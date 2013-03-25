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
package com.codefollower.lealone.test.jdbc.transaction;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

public class TransactionTest extends TestBase {
    Configuration conf = HBaseConfiguration.create();

    @Test
    public void run() throws Exception {
        //stmt.executeUpdate("DROP TABLE IF EXISTS TransactionTest");
        createTableIfNotExists("TransactionTest");
        testCommit();
        testRollback();
        //delete();
    }

    void delete() throws Exception {

        HTable t = new HTable(conf, "TRANSACTIONTEST");

        Put put = new Put(Bytes.toBytes("100"));
        put.add(Bytes.toBytes("CF1"), Bytes.toBytes("f1"), 10, Bytes.toBytes("01"));
        t.put(put);

        put = new Put(Bytes.toBytes("200"));
        put.add(Bytes.toBytes("CF1"), Bytes.toBytes("f1"), 10, Bytes.toBytes("02"));
        t.put(put);

        List<Delete> batch = new ArrayList<Delete>();
        scan();
        //t.delete(new Delete(Bytes.toBytes("01"), 10, null));
        //t.delete(new Delete(Bytes.toBytes("02"), 10, null));

        batch.add(new Delete(Bytes.toBytes("100"), 10, null));
        batch.add(new Delete(Bytes.toBytes("200"), 10, null));

        t.delete(batch);
        scan();
        //Thread.sleep(2000);

        //put = new Put(Bytes.toBytes("01"));
        //put.add(Bytes.toBytes("CF1"), Bytes.toBytes("f1"), System.currentTimeMillis(), Bytes.toBytes("01"));
        //put.add(Bytes.toBytes("CF1"), Bytes.toBytes("f1"), 100, Bytes.toBytes("01"));
        //t.put(put);
    }

    void insert() throws Exception {
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('01', 'a1', 'b', 51)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('02', 'a1', 'b', 61)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('03', 'a1', 'b', 61)");

        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('25', 'a2', 'b', 51)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('26', 'a2', 'b', 61)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('27', 'a2', 'b', 61)");

        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('50', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('51', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('52', 'a1', 'b', 12)");

        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('75', 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('76', 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO TransactionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testCommit() throws Exception {
        try {
            conn.setAutoCommit(false);
            insert();
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        scan();

        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(12, getIntValue(1, true));

        sql = "DELETE FROM TransactionTest";
        assertEquals(12, stmt.executeUpdate(sql));
        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(0, getIntValue(1, true));
    }

    void testRollback() throws Exception {
        try {
            conn.setAutoCommit(false);
            insert();
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }

        scan();
        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(0, getIntValue(1, true));

    }

    void scan() throws Exception {
        HTable t = new HTable(conf, "TRANSACTIONTEST");
        for (Result r : t.getScanner(new Scan())) {
            System.out.println(r);
        }
    }
}
