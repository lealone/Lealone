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
package org.lealone.test.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.lealone.common.util.JdbcUtils;
import org.lealone.test.TestBase;

public class SqlTestBase extends TestBase {

    protected String dbName;
    protected String user;
    protected String password;

    protected Connection conn;
    protected Statement stmt;
    protected ResultSet rs;
    protected String sql;

    protected SqlTestBase() {
        // addConnectionParameter("TRACE_LEVEL_FILE", TraceSystem.ADAPTER + "");
    }

    protected SqlTestBase(String dbName) {
        this.dbName = dbName;
    }

    protected SqlTestBase(String user, String password) {
        this.user = user;
        this.password = password;
    }

    @Before
    public void setUpBefore() {
        try {
            if (dbName != null) {
                conn = getConnection(dbName);
            } else if (user != null) {
                conn = getConnection(user, password);
            } else {
                conn = getConnection();
            }
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDownAfter() {
        JdbcUtils.closeSilently(rs);
        JdbcUtils.closeSilently(stmt);
        JdbcUtils.closeSilently(conn);
    }

    // 不用加@Test，子类可以手工运行，只要实现test方法即可
    public void runTest() {
        setUpBefore();
        try {
            test();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tearDownAfter();
        }
    }

    protected void test() throws Exception {
        // do nothing
    }

    public int executeUpdate(String sql) {
        try {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // e.printStackTrace();
            // return -1;
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate() {
        return executeUpdate(sql);
    }

    public void tryExecuteUpdate() {
        tryExecuteUpdate(sql);
    }

    public void tryExecuteUpdate(String sql) {
        try {
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void createTable(String tableName) {
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
        executeUpdate("CREATE TABLE " + tableName + " (pk varchar(100) NOT NULL PRIMARY KEY, " + //
                "f1 varchar(100), f2 varchar(100), f3 int)");
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

    public void executeQuery() {
        try {
            rs = stmt.executeQuery(sql);
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeResultSet() throws Exception {
        rs.close();
        rs = null;
    }

    public boolean next() throws Exception {
        check();
        return rs.next();
    }

    public int printResultSet() {
        int count = 0;
        try {
            rs = stmt.executeQuery(sql);

            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= n; i++) {
                    System.out.print(rs.getString(i) + " ");
                }
                count++;
                System.out.println();
            }
            rs.close();
            rs = null;
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
}
