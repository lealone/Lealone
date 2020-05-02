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
package org.lealone.test.db;

import java.sql.Connection;
import java.sql.ResultSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.Role;
import org.lealone.db.auth.User;
import org.lealone.db.result.Result;
import org.lealone.db.result.SearchRow;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSessionFactory;
import org.lealone.test.UnitTestBase;

public class DbObjectTestBase extends UnitTestBase {
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
        addConnectionParameter("DATABASE_TO_UPPER", "false"); // 不转成大写
        session = createSession();
        db = session.getDatabase();
        schema = db.findSchema(session, Constants.SCHEMA_MAIN);
    }

    public ServerSession createSession() {
        ConnectionInfo ci = new ConnectionInfo(getURL(dbName));
        return (ServerSession) ServerSessionFactory.getInstance().createSession(ci).get();
    }

    public int executeUpdate(String sql) {
        return session.prepareStatementLocal(sql).executeUpdate().get();
    }

    public Result executeQuery(String sql) {
        return session.prepareStatementLocal(sql).executeQuery(0, false).get();
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

    public void assertException(Exception e, int expectedErrorCode) {
        assertTrue(e instanceof DbException);
        assertEquals(expectedErrorCode, ((DbException) e).getErrorCode());
        // p(e.getMessage());
    }
}
