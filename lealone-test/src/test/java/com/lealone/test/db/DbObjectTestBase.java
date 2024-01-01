/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import java.sql.Connection;
import java.sql.ResultSet;

import org.junit.Before;

import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.auth.Role;
import com.lealone.db.auth.User;
import com.lealone.db.result.Result;
import com.lealone.db.result.SearchRow;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;

public class DbObjectTestBase extends DbTestBase {

    public static final String DB_NAME = "DbObjectTest";

    protected Database db;
    protected ServerSession session;
    protected Schema schema;

    protected String sql;

    public DbObjectTestBase() {
        this(DB_NAME);
    }

    public DbObjectTestBase(String dbName) {
        this.dbName = dbName;
        // setInMemory(true);
        setEmbedded(true);
        // enableTrace();
        addConnectionParameter("DATABASE_TO_UPPER", "false"); // 不转成大写
        session = createSession();
        db = session.getDatabase();
        schema = db.findSchema(session, Constants.SCHEMA_MAIN);
    }

    @Override
    @Before
    public void setUpBefore() {
        session.setAutoCommit(true);
    }

    public ServerSession createSession() {
        ConnectionInfo ci = new ConnectionInfo(getURL(dbName));
        return (ServerSession) getServerSessionFactory().createSession(ci).get();
    }

    public int executeUpdate() {
        return executeUpdate(sql);
    }

    public int executeUpdate(String sql) {
        return session.executeUpdateLocal(sql);
    }

    public Result executeQuery(String sql) {
        return session.executeQueryLocal(sql, 0, false);
    }

    // index从1开始
    public int getInt(Result result, int index) {
        return result.currentRow()[index - 1].getInt();
    }

    public int getInt(String sql, int index) {
        Result result = executeQuery(sql);
        if (result.next())
            return result.currentRow()[index - 1].getInt();
        else
            return -1;
    }

    public String getString(Result result, int index) {
        return result.currentRow()[index - 1].getString();
    }

    public String getString(String sql, int index) {
        Result result = executeQuery(sql);
        if (result.next())
            return result.currentRow()[index - 1].getString();
        else
            return null;
    }

    public int printResultSet(String sql) {
        int count = 0;
        try {
            Connection conn = getConnection();
            ResultSet rs = conn.createStatement().executeQuery(sql);
            count = printResultSet(rs);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public Database findDatabase(String dbName) {
        return LealoneDatabase.getInstance().findDatabase(dbName);
    }

    public User findUser(String userName) {
        return db.findUser(session, userName);
    }

    public Role findRole(String roleName) {
        return db.findRole(session, roleName);
    }

    public SearchRow findMeta(int id) {
        return db.findMeta(session, id);
    }

    public Table findTable(String tableName) {
        return schema.findTableOrView(session, tableName);
    }
}
