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
package com.codefollower.yourbase.test.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class BenchWrite {
    public static void main(String[] args) throws Exception {
        new BenchWrite().run();
    }

    Connection conn;
    Statement stmt;
    PreparedStatement ps;

    byte[] cf = b("CF");
    byte[] id = b("ID");
    byte[] name = b("NAME");
    byte[] age = b("AGE");
    byte[] salary = b("SALARY");

    Configuration conf = HBaseConfiguration.create();
    HTable t;

    byte[] b(String v) {
        return Bytes.toBytes(v);
    }

    byte[] b(long v) {
        return Bytes.toBytes(v);
    }

    byte[] b(int v) {
        return Bytes.toBytes(v);
    }

    byte[] b(float v) {
        return Bytes.toBytes(v);
    }

    void run() throws Exception {
        init();
        createTable();
        initHTable();
        initPreparedStatement();

        int count = 10;

        p();

        for (int i = 0; i < count; i++) {
            testStatement();
        }

        p();

        for (int i = 0; i < count; i++) {
            testPreparedStatement();
        }

        p();

        for (int i = 0; i < count; i++) {
            testHBase();
        }

        p();

        for (int i = 0; i < count; i++) {
            testHBaseBatch();
        }
    }

    void init() throws Exception {
        String url = "jdbc:h2:tcp://localhost:9092/hbasedb;STORE_ENGINE_NAME=HBASE";
        conn = DriverManager.getConnection(url, "sa", "");
        stmt = conn.createStatement();
        stmt.executeUpdate("SET DB_CLOSE_DELAY -1"); //不马上关闭数据库

    }

    void initHTable() throws Exception {
        t = new HTable(conf, b("BENCHWRITE"));
    }

    void initPreparedStatement() throws Exception {
        ps = conn.prepareStatement("INSERT INTO BenchWrite(_rowkey_, id, name, age, salary) VALUES(?, ?, ?, ?, ?)");
    }

    void createTable() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS BenchWrite(" //
                + "COLUMN FAMILY cf(id int, name varchar(500), age long, salary float))");
    }

    int count = 10200;

    void testHBase() throws Exception {
        long start = System.nanoTime();
        for (int i = 10000; i < count; i++) {
            Put put = new Put(b("RK" + i));
            put.add(cf, id, b(i));
            put.add(cf, name, b("zhh-2009"));
            put.add(cf, age, b(30L));
            put.add(cf, salary, b(3000.50F));
            t.put(put);
        }
        long end = System.nanoTime();
        p("testHBase()", end - start);
    }

    void testHBaseBatch() throws Exception {
        List<Put> puts = new ArrayList<Put>();

        long start = System.nanoTime();
        for (int i = 10000; i < count; i++) {
            Put put = new Put(b("RK" + i));
            put.add(cf, id, b(i));
            put.add(cf, name, b("zhh-2009"));
            put.add(cf, age, b(30L));
            put.add(cf, salary, b(3000.50F));
            puts.add(put);
        }
        t.put(puts);
        long end = System.nanoTime();
        p("testHBaseBatch()", end - start);
    }

    void p(String m, long v) {
        System.out.println(m + ": " + v / 1000000 + " ms");
    }

    void p() {
        System.out.println();
    }

    void testPreparedStatement() throws Exception {
        long start = System.nanoTime();
        for (int i = 10000; i < count; i++) {
            ps.setString(1, "RK" + i);
            ps.setInt(2, i);
            ps.setString(3, "zhh-2009");
            ps.setLong(4, 30L);
            ps.setFloat(5, 3000.50F);
            ps.executeUpdate();
        }
        long end = System.nanoTime();
        p("testPreparedStatement()", end - start);
    }

    void testStatement() throws Exception {
        long start = System.nanoTime();
        StringBuilder s = null;
        for (int i = 10000; i < count; i++) {
            s = new StringBuilder("INSERT INTO BenchWrite(_rowkey_, id, name, age, salary) VALUES(");
            s.append("'RK").append(i).append("',");
            s.append(i).append(",");
            s.append("'zhh-2009',");
            s.append(30L).append(",");
            s.append(3000.50F).append(")");
            stmt.executeUpdate(s.toString());
        }
        long end = System.nanoTime();
        p("testStatement()", end - start);
    }
}
