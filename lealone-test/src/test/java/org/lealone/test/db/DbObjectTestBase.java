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

import org.lealone.common.message.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Auth;
import org.lealone.db.auth.Role;
import org.lealone.db.auth.User;
import org.lealone.db.result.Result;
import org.lealone.db.result.SearchRow;
import org.lealone.db.schema.Schema;
import org.lealone.test.UnitTestBase;

public class DbObjectTestBase extends UnitTestBase {
    public static final String DB_NAME = "DbObjectTest";

    protected Database db;
    protected ServerSession session;
    protected Schema schema;

    protected String sql;

    public DbObjectTestBase() {
        setInMemory(true);
        setEmbedded(true);
        addConnectionParameter("DATABASE_TO_UPPER", "false"); // 不转成大写
        ConnectionInfo ci = new ConnectionInfo(getURL(DB_NAME));
        session = DatabaseEngine.createSession(ci);
        db = session.getDatabase();
        // session = db.getSystemSession();
        schema = db.findSchema(Constants.SCHEMA_MAIN);
    }

    public int executeUpdate(String sql) {
        return session.prepareStatementLocal(sql).executeUpdate();
    }

    public Result executeQuery(String sql) {
        return session.prepareStatementLocal(sql).executeQuery(0, false);
    }

    // index从1开始
    public int getInt(Result result, int index) {
        if (result.next())
            return result.currentRow()[index - 1].getInt();
        else
            return -1;
    }

    public int getInt(String sql, int index) {
        return getInt(executeQuery(sql), index);
    }

    public Database findDatabase(String dbName) {
        return LealoneDatabase.getInstance().findDatabase(dbName);
    }

    public User findUser(String userName) {
        return Auth.findUser(userName);
    }

    public Role findRole(String roleName) {
        return Auth.findRole(roleName);
    }

    public SearchRow findMeta(int id) {
        return db.findMeta(session, id);
    }

    public SearchRow findMeta(Database db, int id) {
        return db.findMeta(session, id);
    }

    public void assertException(Exception e, int expectedErrorCode) {
        assertTrue(e instanceof DbException);
        assertEquals(expectedErrorCode, ((DbException) e).getErrorCode());
        // p(e.getMessage());
    }
}
