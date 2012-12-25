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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestBase {
    protected static Connection conn;
    protected static Statement stmt;
    protected ResultSet rs;
    protected String sql;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        //HBase不需要在URL中指定数据库名
        String url = "jdbc:h2:hbase:";
        conn = DriverManager.getConnection(url, "sa", "");
        stmt = conn.createStatement();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (stmt != null)
            stmt.close();
        if (conn != null)
            conn.close();
    }

    public static String toS(byte[] v) {
        return Bytes.toString(v);
    }

    public void createTableSQL(String sql) throws Exception {
        stmt.executeUpdate(sql);
        Thread.sleep(2000); //TODO CREATE表太慢未完成时，因为是异步的接下来的操作有时会抛异常(比如找不到表)
    }

    //TODO 这个方法很慢
    public void createTable(String tableName, String... splitKeys) throws Exception {
        //stmt.executeUpdate("DROP HBASE TABLE IF EXISTS " + tableName);
        //Thread.sleep(2000); //TODO drop表太慢未完成时，因为是异步的接下来的建表操作有时会抛异常

        StringBuilder builder = new StringBuilder();
        for (String s : splitKeys) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append("'").append(s).append("'");
        }

        String splitKeyStr = "";
        if (splitKeys.length > 0) {
            splitKeyStr = "SPLIT KEYS(" + builder + "), ";
        }

        //CREATE HBASE TABLE语句不用定义字段
        createTableSQL("CREATE HBASE TABLE IF NOT EXISTS " + tableName + " (" //
                //此OPTIONS对应org.apache.hadoop.hbase.HTableDescriptor的参数选项
                + "OPTIONS(DEFERRED_LOG_FLUSH='false'), "

                //预分region
                + splitKeyStr

                //COLUMN FAMILY中的OPTIONS对应org.apache.hadoop.hbase.HColumnDescriptor的参数选项
                + "COLUMN FAMILY cf1 (OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)), " //

                + "COLUMN FAMILY cf2 (OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true))" //
                + ")");

        Configuration conf = HBaseConfiguration.create();
        //确保表已可用
        while (true) {
            if (ZKTableReadOnly.isEnabledTable(new ZooKeeperWatcher(conf, "TestBase", null), tableName.toUpperCase()))
                break;
            Thread.sleep(100);
        }
        //Thread.sleep(2000); //TODO CREATE表太慢未完成时，因为是异步的接下来的操作有时会抛异常(比如找不到表)

        //TODO H2数据库会默认把标识符转成大写，这个问题未解决，所以这里表名、列族名用大写
        HTable t = new HTable(conf, tableName.toUpperCase());
        Assert.assertEquals(splitKeys.length + 1, t.getRegionLocations().size());
        t.close();
    }

    private void check() throws Exception {
        if (rs == null)
            executeQuery();
    }

    public int getIntValue(int i) throws Exception {
        check();
        return rs.getInt(i);
    }

    public int getIntValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getInt(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public long getLongValue(int i) throws Exception {
        check();
        return rs.getLong(i);
    }

    public long getLongValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getLong(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public double getDoubleValue(int i) throws Exception {
        check();
        return rs.getDouble(i);
    }

    public double getDoubleValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getDouble(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public String getStringValue(int i) throws Exception {
        check();
        return rs.getString(i);
    }

    public String getStringValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getString(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public boolean getBooleanValue(int i) throws Exception {
        check();
        return rs.getBoolean(i);
    }

    public boolean getBooleanValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getBoolean(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public void executeQuery() throws Exception {
        rs = stmt.executeQuery(sql);
        rs.next();
    }

    public void closeResultSet() throws Exception {
        rs.close();
        rs = null;
    }

    public boolean next() throws Exception {
        check();
        return rs.next();
    }

    public void printResultSet() throws Exception {
        rs = stmt.executeQuery(sql);

        int n = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= n; i++) {
                System.out.print(rs.getString(i) + " ");
            }
            System.out.println();
        }
        rs.close();
        rs = null;
        System.out.println();
    }
}
