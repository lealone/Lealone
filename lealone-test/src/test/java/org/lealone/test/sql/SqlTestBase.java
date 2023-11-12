/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.JdbcUtils;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.test.LealoneStart;
import org.lealone.test.TestBase;

public class SqlTestBase extends TestBase implements TestBase.SqlExecutor, TestBase.ClientServerTest {

    protected Connection conn;
    protected Statement stmt;
    protected ResultSet rs;
    protected String sql;
    protected RunMode runMode;
    protected boolean databaseToUpper = true;

    protected SqlTestBase() {
        // addConnectionParameter("TRACE_LEVEL_FILE", TraceSystem.ADAPTER + "");
    }

    protected SqlTestBase(String dbName) {
        this.dbName = dbName;
    }

    protected SqlTestBase(String dbName, RunMode runMode) {
        this.dbName = dbName;
        this.runMode = runMode;
    }

    protected SqlTestBase(String user, String password) {
        this.user = user;
        this.password = password;
    }

    protected Throwable getRootCause(Throwable cause) {
        return DbException.getRootCause(cause);
    }

    protected boolean autoStartTcpServer() {
        return false;
    }

    private static boolean tcpServerStarted = false;
    private static boolean testDatabaseCreated = false;

    @Before
    public void setUpBefore() {
        if (autoStartTcpServer()) {
            synchronized (getClass()) {
                if (!tcpServerStarted) {
                    tcpServerStarted = true;
                    LealoneStart.run();
                }
            }
        }
        if (!testDatabaseCreated) {
            synchronized (getClass()) {
                if (!testDatabaseCreated) {
                    if (!LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
                        createTestDatabase();
                        testDatabaseCreated = true;
                    }
                }
            }
        }

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
            throw DbException.convert(e);
        }
    }

    private boolean createTestDatabase() {
        String dbName = this.dbName;
        if (dbName == null)
            dbName = TestBase.DEFAULT_DB_NAME;
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
        if (runMode != null)
            sql += " RUN MODE " + runMode;
        sql += " PARAMETERS(database_to_upper=" + databaseToUpper + ")";
        final String createDatabase = sql;
        class CDB extends SqlTestBase {
            public CDB() {
                super(LealoneDatabase.NAME);
            }

            @Override
            protected void test() throws Exception {
                stmt.executeUpdate(createDatabase);
            }
        }
        return new CDB().runTest();
    }

    @After
    public void tearDownAfter() {
        JdbcUtils.closeSilently(rs);
        JdbcUtils.closeSilently(stmt);
        JdbcUtils.closeSilently(conn);
    }

    // 不用加@Test，子类可以手工运行，只要实现test方法即可
    public boolean runTest() {
        setUpBefore();
        try {
            test();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
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
        executeQuery(sql);
    }

    public void executeQuery(String sql) {
        try {
            rs = stmt.executeQuery(sql);
            rs.next();
        } catch (SQLException e) {
            throw DbException.convert(e);
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
            count = printResultSet(rs);
            rs = null;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return count;
    }

    @Override
    public void execute(String sql) {
        try {
            stmt.execute(sql);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public void executeUpdateThanAssertErrorCode(String sql, int errorCode) {
        try {
            stmt.executeUpdate(sql);
            fail(sql);
        } catch (Exception e) {
            assertErrorCode(e, errorCode);
        }
    }

    public void executeQueryThanAssertErrorCode(String sql, int errorCode) {
        try {
            rs = stmt.executeQuery(sql);
            rs.next();
            fail(sql);
        } catch (Exception e) {
            assertErrorCode(e, errorCode);
        }
    }
}
