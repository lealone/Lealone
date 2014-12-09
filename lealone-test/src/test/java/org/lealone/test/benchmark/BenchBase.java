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
package org.lealone.test.benchmark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

public class BenchBase {
    protected static final Configuration conf = HBaseConfiguration.create();
    protected static final HTablePool tablePool = new HTablePool(conf, conf.getInt("hbase.htable.pool.max", 100));

    protected String tableName;
    protected int loop = 10;
    protected long total;

    protected Connection conn;
    protected Statement stmt;
    protected PreparedStatement ps;

    public BenchBase() {
    }

    public BenchBase(String tableName) {
        this.tableName = tableName;
    }

    public void init() throws Exception {
        String url = "jdbc:lealone:tcp://localhost:5210/hbasedb";
        conn = DriverManager.getConnection(url, "sa", "");
        stmt = conn.createStatement();
        stmt.executeUpdate("SET DB_CLOSE_DELAY -1"); //不马上关闭数据库
    }

    public HTableInterface getHTable() {
        return (HTableInterface) tablePool.getTable(tableName);
    }

    public void createTable(String... familyNames) throws IOException {
        createTable(Compression.Algorithm.GZ, familyNames);
    }

    public void createTable(Compression.Algorithm compressionType, String... familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            hcd.setCompressionType(compressionType);
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            hcd.setMaxVersions(3);
            hcd.setMinVersions(1);
            htd.addFamily(hcd);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
        admin.close();
    }

    public void createTable(HColumnDescriptor... columnDescriptors) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (HColumnDescriptor columnDescriptor : columnDescriptors) {
            if (columnDescriptor.getDataBlockEncoding() == DataBlockEncoding.NONE)
                columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(columnDescriptor);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
        admin.close();
    }

    public void createTable(byte[][] splitKeys, HColumnDescriptor... columnDescriptors) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (HColumnDescriptor columnDescriptor : columnDescriptors) {
            if (columnDescriptor.getDataBlockEncoding() == DataBlockEncoding.NONE)
                columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(columnDescriptor);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd, splitKeys);
        }
        admin.close();
    }

    public void deleteTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(tableName)) {
            admin.close();
            return;
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    public byte[] b(String v) {
        return Bytes.toBytes(v);
    }

    public byte[] b(long v) {
        return Bytes.toBytes(v);
    }

    public byte[] b(int v) {
        return Bytes.toBytes(v);
    }

    public byte[] b(float v) {
        return Bytes.toBytes(v);
    }

    public byte[] b(double v) {
        return Bytes.toBytes(v);
    }

    public String s(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    public void p(Object o) {
        System.out.println(o);
    }

    public void p() {
        System.out.println();
    }

    public void p(String m, long v) {
        System.out.println(m + ": " + v / 1000000 + " ms");
    }

    public void avg() {
        p("----------------------------");
        p("loop: " + loop + ", avg", total / loop);
        p();
        total = 0;
    }

}
