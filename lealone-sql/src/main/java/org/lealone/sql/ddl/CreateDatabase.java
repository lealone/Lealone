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

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.CommandInterface;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.Session;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DefineCommand {

    private String dbName;
    private boolean ifNotExists;
    private boolean temporary;

    public CreateDatabase(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setDatabaseName(String dbName) {
        this.dbName = dbName;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
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
        Database newDb = new Database(id, dbName);
        newDb.setTemporary(temporary);
        db.addDatabaseObject(session, newDb);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_DATABASE;
    }

}
