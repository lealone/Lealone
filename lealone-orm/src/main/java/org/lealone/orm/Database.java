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
package org.lealone.orm;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.ServerSession;

public class Database {

    private final org.lealone.db.Database db;
    private final ServerSession session;

    public Database(String url) {
        ConnectionInfo ci = new ConnectionInfo(url);
        session = DatabaseEngine.createSession(ci);
        db = session.getDatabase();
    }

    public int executeUpdate(String sql) {
        return session.parseStatement(sql).executeUpdate();
    }

    public Table getTable(String tableName) {
        return new Table(this, getDbTable(tableName));
    }

    org.lealone.db.table.Table getDbTable(String tableName) {
        if (db.getSettings().databaseToUpper) {
            tableName = tableName.toUpperCase();
        }
        int dotPos = tableName.indexOf('.');
        String schemaName = Constants.SCHEMA_MAIN;
        if (dotPos > -1) {
            schemaName = tableName.substring(0, dotPos);
            tableName = tableName.substring(dotPos + 1);
        }
        return db.getSchema(schemaName).getTableOrView(session, tableName);
    }

    ServerSession getSession() {
        return session;
    }

    public void close() {
        db.getTransactionEngine().close();
    }
}
