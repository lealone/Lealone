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
package org.lealone.cassandra.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestBase {
    protected static Connection conn;
    protected static Statement stmt;
    protected ResultSet rs;
    protected String sql;
    protected String tableName;
    protected String db = "hbasedb";
    //protected static String url = "jdbc:lealone:tcp://localhost:5210/hbasedb;DATABASE_TO_UPPER=false";
    //protected static String url = "jdbc:lealone:tcp://localhost:5210,localhost:5211/hbasedb;USE_H2_CLUSTER_MODE=true";
    protected static String url = "jdbc:lealone:tcp://localhost:5210/hbasedb";

    public static String getURL() {
        return url;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conn = DriverManager.getConnection(getURL(), "sa", "");
        stmt = conn.createStatement();

        //TODO Cassandra的语法不符号JDBC转义语法的要求
        stmt.setEscapeProcessing(false);

        String sql = "CREATE KEYSPACE IF NOT EXISTS TestKeyspace " + //
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true";
        stmt.executeUpdate(sql);
        stmt.executeUpdate("USE TestKeyspace");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (stmt != null)
            stmt.close();
        if (conn != null)
            conn.close();
    }

    public void createTableSQL(String sql) throws Exception {
        stmt.executeUpdate(sql);
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

    public int executeUpdate() throws Exception {
        return stmt.executeUpdate(sql);
    }

    public int tryExecuteUpdate() throws Exception {
        try {
            return stmt.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println("Try Execute SQL: " + sql);
            System.out.println("Exception: " + e.getMessage());
            //e.printStackTrace();
            return -1;
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
