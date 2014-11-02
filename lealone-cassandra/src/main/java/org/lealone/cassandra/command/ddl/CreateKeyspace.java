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
package org.lealone.cassandra.command.ddl;

import org.apache.cassandra.cql3.KSPropDefs;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.service.MigrationManager;
import org.lealone.command.CommandInterface;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.User;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

public class CreateKeyspace extends DefineCommand {

    private String keyspaceName;
    private String authorization;
    private boolean ifNotExists;

    private KSPropDefs defs;

    public CreateKeyspace(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public void setAuthorization(String userName) {
        this.authorization = userName;
    }

    public void setKSPropDefs(KSPropDefs defs) {
        this.defs = defs;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SCHEMA;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        User user = db.getUser(authorization);
        user.checkAdmin();
        if (db.findSchema(keyspaceName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, keyspaceName);
        }
        int id = getObjectId();
        Schema schema = new Schema(db, id, keyspaceName, user, false);
        db.addDatabaseObject(session, schema);

        try {
            MigrationManager.announceNewKeyspace(defs.asKSMetadata(keyspaceName));
        } catch (AlreadyExistsException e) {
            if (!ifNotExists)
                throw DbException.convert(e);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return 0;
    }
}
