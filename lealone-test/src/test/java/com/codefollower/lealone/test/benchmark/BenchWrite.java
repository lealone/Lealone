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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class BenchWrite extends BenchBase {
    byte[] cf = b("CF");
    byte[] id = b("ID");
    byte[] name = b("NAME");
    byte[] age = b("AGE");
    byte[] salary = b("SALARY");

    HTable t;
    int startKey;
    int endKey;

    //endKey-startKey的值就是一个事务中包含的写操作个数
    public BenchWrite(String tableName, int startKey, int endKey) {
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @Override
    public void avg() {
        p("----------------------------");
        p("rows: " + (endKey - startKey) + ", loop: " + loop + ", avg", total / loop);
        p();
        total = 0;
    }

    public void run() throws Exception {
        init();
        createTable();
        initHTable();
        initPreparedStatement();

        for (int i = 0; i < loop; i++) {
            total += testStatement();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testStatement();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testPreparedStatement();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testTransactionalStatement();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testTransactionalPreparedStatement();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testHBase();
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testHBaseBatch();
        }
        avg();

        new HBaseAdmin(conf).flush(tableName.toUpperCase());
        regions();
        //scan();
    }

    public void regions() throws Exception {
        HTable t = new HTable(conf, tableName.toUpperCase());
        for (Map.Entry<HRegionInfo, ServerName> e : t.getRegionLocations().entrySet()) {
            HRegionInfo info = e.getKey();
            System.out.println("info.getEncodedName()=" + info.getEncodedName());
            ServerName server = e.getValue();

            System.out.println("HRegionInfo = " + info.getRegionNameAsString());
            System.out.println("ServerName = " + server);
            System.out.println();
        }
    }

    void scan() throws Exception {
        HTable t = new HTable(conf, tableName.toUpperCase());
        for (Result r : t.getScanner(new Scan())) {
            System.out.println("rowKey: " + Bytes.toString(r.getRow()) + ", value: " + r);
        }
    }

    void initHTable() throws Exception {
        t = new HTable(conf, b(tableName.toUpperCase()));
    }

    void initPreparedStatement() throws Exception {
        ps = conn.prepareStatement("INSERT INTO " + tableName + "(_rowkey_, id, name, age, salary) VALUES(?, ?, ?, ?, ?)");
    }

    public abstract void createTable() throws Exception;

    long testHBase() throws Exception {
        long start = System.nanoTime();
        for (int i = startKey; i < endKey; i++) {
            Put put = new Put(b("RK" + i));
            put.add(cf, id, 2, b(i));
            put.add(cf, name, 2, b("zhh-2009"));
            put.add(cf, age, 2, b(30L));
            put.add(cf, salary, 2, b(3000.50D));
            t.put(put);
        }
        long end = System.nanoTime();
        p("testHBase()", end - start);

        return end - start;
    }

    long testHBaseBatch() throws Exception {
        List<Put> puts = new ArrayList<Put>();

        long start = System.nanoTime();
        for (int i = startKey; i < endKey; i++) {
            Put put = new Put(b("RK" + i));
            put.add(cf, id, 2, b(i));
            put.add(cf, name, 2, b("zhh-2009"));
            put.add(cf, age, 2, b(30L));
            put.add(cf, salary, 2, b(3000.50D));
            puts.add(put);

            if (puts.size() > 200) {
                t.put(puts);
                puts.clear();
            }
        }
        t.put(puts);
        long end = System.nanoTime();
        p("testHBaseBatch()", end - start);
        return end - start;
    }

    long testPreparedStatement() throws Exception {
        long start = System.nanoTime();
        for (int i = startKey; i < endKey; i++) {
            ps.setString(1, "RK" + i);
            ps.setInt(2, i);
            ps.setString(3, "zhh-2009");
            ps.setLong(4, 30L);
            ps.setDouble(5, 3000.50D);
            ps.executeUpdate();
        }
        long end = System.nanoTime();
        p("testPreparedStatement()", end - start);

        return end - start;
    }

    long testStatement() throws Exception {
        long start = System.nanoTime();
        StringBuilder s = null;
        for (int i = startKey; i < endKey; i++) {
            s = new StringBuilder("INSERT INTO " + tableName + "(_rowkey_, id, name, age, salary) VALUES(");
            s.append("'RK").append(i).append("',");
            s.append(i).append(",");
            s.append("'zhh-2009',");
            s.append(30L).append(",");
            s.append(3000.50D).append(")");
            stmt.executeUpdate(s.toString());
        }
        long end = System.nanoTime();
        p("testStatement()", end - start);

        return end - start;
    }

    long testTransactionalPreparedStatement() throws Exception {
        conn.setAutoCommit(false);
        long start = System.nanoTime();
        for (int i = startKey; i < endKey; i++) {
            ps.setString(1, "RK" + i);
            ps.setInt(2, i);
            ps.setString(3, "zhh-2009");
            ps.setLong(4, 30L);
            ps.setDouble(5, 3000.50D);
            ps.executeUpdate();
        }
        conn.commit();
        long end = System.nanoTime();
        p("testTransactionalPreparedStatement()", end - start);
        conn.setAutoCommit(true);

        return end - start;
    }

    long testTransactionalStatement() throws Exception {
        conn.setAutoCommit(false);
        long start = System.nanoTime();
        StringBuilder s = null;
        for (int i = startKey; i < endKey; i++) {
            s = new StringBuilder("INSERT INTO " + tableName + "(_rowkey_, id, name, age, salary) VALUES(");
            s.append("'RK").append(i).append("',");
            s.append(i).append(",");
            s.append("'zhh-2009',");
            s.append(30L).append(",");
            s.append(3000.50D).append(")");
            stmt.executeUpdate(s.toString());
        }
        conn.commit();
        long end = System.nanoTime();
        p("testTransactionalStatement()", end - start);
        conn.setAutoCommit(true);

        return end - start;
    }
}
