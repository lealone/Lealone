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
package org.lealone.sql.ddl;

import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DefineStatement {

    private final String dbName;
    private final boolean ifNotExists;
    private final Map<String, String> parameters;
    private final Map<String, String> replicationProperties;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, Map<String, String> parameters,
            Map<String, String> replicationProperties) {
        super(session);
        this.dbName = dbName;
        this.ifNotExists = ifNotExists;
        this.parameters = parameters;
        this.replicationProperties = replicationProperties;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        LealoneDatabase db = LealoneDatabase.getInstance();
        if (db.findDatabase(dbName) != null || LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DATABASE_ALREADY_EXISTS_1, dbName);
        }
        int id = getObjectId(db);
        Database newDb = new Database(id, dbName, parameters);
        newDb.setReplicationProperties(replicationProperties);
        db.addDatabaseObject(session, newDb);
        return 0;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DATABASE;
    }

}
