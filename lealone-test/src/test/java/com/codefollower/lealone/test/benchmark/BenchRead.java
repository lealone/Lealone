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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public abstract class BenchRead extends BenchWrite {

    public BenchRead(String tableName, int startKey, int endKey) {
        super(tableName, startKey, endKey);
    }

    public void p_ns(String m, long v) {
        System.out.println(m + ": " + v + " ns");
    }

    public void avg_ns() {
        p("----------------------------");
        p_ns("rows: " + (endKey - startKey) + ", loop: " + loop + ", avg", total / loop);
        p();
        total = 0;
    }

    public void run() throws Exception {
        init();
        createTable();
        initHTable();

        ps = conn.prepareStatement("select * from " + tableName + " where _rowkey_=?");

        ResultSet rs = stmt.executeQuery("select * from " + tableName + " where _rowkey_='RK" + startKey + "'");
        if (!rs.next())
            testHBaseBatch();
        rs.close();

        p();
        int step = 1000;

        loop = (endKey - startKey) / step;

        //预热
        for (int j = startKey; j < endKey; j += step) {
            total += testGetStatement(j);
        }
        avg_ns();

        for (int j = startKey; j < endKey; j += step) {
            total += testGetStatement(j);
        }
        avg_ns();

        for (int j = startKey; j < endKey; j += step) {
            total += testGetPreparedStatement(j);
        }
        avg_ns();

        for (int j = startKey; j < endKey; j += step) {
            total += testGetHBase(j);
        }
        avg_ns();

        ps.close();
        ps = conn.prepareStatement("select * from " + tableName + " where _rowkey_>=?");
        step = 100;
        startKey = endKey - 1000;
        loop = (endKey - startKey) / step;

        for (int j = startKey; j < endKey; j += step) {
            total += testScanStatement(j);
        }
        avg();

        for (int j = startKey; j < endKey; j += step) {
            total += testScanPreparedStatement(j);
        }
        avg();

        for (int j = startKey; j < endKey; j += step) {
            total += testScanHBase(j);
        }
        avg();

    }

    long testGetStatement(int rk) throws Exception {
        long start = System.nanoTime();
        stmt.executeQuery("select * from " + tableName + " where _rowkey_='RK" + rk + "'");
        long end = System.nanoTime();
        p_ns("testGetStatement()", end - start);
        return end - start;
    }

    long testGetPreparedStatement(int rk) throws Exception {
        long start = System.nanoTime();
        ps.setString(1, "RK" + rk);
        ps.executeQuery();
        long end = System.nanoTime();
        p_ns("testGetPreparedStatement()", end - start);
        return end - start;
    }

    long testGetHBase(int rk) throws Exception {
        byte[] rowKey = b("RK" + rk);
        long start = System.nanoTime();
        Get get = new Get(rowKey);
        t.get(get);
        long end = System.nanoTime();
        p_ns("testGetHBase()", end - start);
        return end - start;
    }

    long testScanStatement(int rk) throws Exception {
        long start = System.nanoTime();
        ResultSet rs = stmt.executeQuery("select * from " + tableName + " where _rowkey_>='RK" + rk + "'");
        int count = 0;
        while (rs.next())
            count++;
        long end = System.nanoTime();
        rs.close();
        p("testScanStatement() count=" + count, end - start);
        return end - start;
    }

    long testScanPreparedStatement(int rk) throws Exception {
        long start = System.nanoTime();
        ps.setString(1, "RK" + rk);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next())
            count++;
        long end = System.nanoTime();
        rs.close();
        p("testScanPreparedStatement() count=" + count, end - start);
        return end - start;
    }

    long testScanHBase(int rk) throws Exception {
        Scan scan = new Scan();
        byte[] rowKey = b("RK" + rk);
        scan.setStartRow(rowKey);
        long start = System.nanoTime();
        ResultScanner rs = t.getScanner(scan);
        int count = 0;
        while (rs.next() != null)
            count++;
        long end = System.nanoTime();
        rs.close();
        p("testScanHBase() count=" + count, end - start);
        return end - start;
    }
}
