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
package com.codefollower.lealone.test.benchmark;

import java.sql.ResultSet;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

public class BenchAggregation extends BenchWrite {
    public static void main(String[] args) throws Exception {
        new BenchAggregation(100000, 200000).run();
    }

    private AggregationClient ac;
    private byte[] tableNameAsBytes;
    private LongColumnInterpreter ci;
    private Scan scan;

    public BenchAggregation(int startKey, int endKey) {
        super("BENCHAGGREGATION", startKey, endKey);
        loop = 3;
    }

    public void run() throws Exception {
        tableNameAsBytes = b(tableName);
        ci = new LongColumnInterpreter();
        scan = new Scan();
        scan.addFamily(b("CF"));

        init();
        createTable();
        initHTable();
        ac = new AggregationClient(conf);

        testHBaseBatch();

        for (int i = 0; i < loop; i++) {
            total += testCount();
        }
        avg();

        stmt.setFetchSize(10000);
        for (int i = 0; i < loop; i++) {
            total += testCount();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testHBaseCount();
        }
        avg();

        //new HBaseAdmin(conf).flush(tableName);

    }

    @Override
    public void createTable() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS " + tableName + " (" //
                + "SPLIT KEYS('RK120000','RK140000','RK160000','RK180000'), " //预分region
                + "COLUMN FAMILY cf(id int, name varchar(500), age long, salary double))");
    }

    long testCount() throws Exception {
        long start = System.nanoTime();
        //ResultSet r = stmt.executeQuery("select count(*) from BENCHAGGREGATION where _rowkey_<'RK180000'");
        ResultSet r = stmt.executeQuery("select count(*) from BENCHAGGREGATION");
        long end = System.nanoTime();
        p("testCount()", end - start);
        r.next();
        p("rowCount=" + r.getInt(1));
        r.close();
        return end - start;
    }

    long testHBaseCount() throws Exception {
        long start = System.nanoTime();
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(tableNameAsBytes, ci, scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        p("testHBaseCount()", end - start);
        p("rowCount=" + rowCount);

        return end - start;
    }
}
