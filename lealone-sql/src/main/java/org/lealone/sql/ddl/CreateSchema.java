/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Auth;
import org.lealone.db.auth.User;
import org.lealone.db.schema.Schema;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE SCHEMA
 */
public class CreateSchema extends DefineStatement {

    private String schemaName;
    private String authorization;
    private boolean ifNotExists;

    private Map<String, String> replicationProperties;

    public CreateSchema(ServerSession session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        User user = Auth.getUser(authorization);
        user.checkSchemaAdmin();

        if (db.findSchema(schemaName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, schemaName);
        }
        int id = getObjectId();
        Schema schema = new Schema(db, id, schemaName, user, false);
        schema.setReplicationProperties(replicationProperties);
        db.addDatabaseObject(session, schema);
        return 0;
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    public void setAuthorization(String userName) {
        this.authorization = userName;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_SCHEMA;
    }

    public void setReplicationProperties(Map<String, String> replicationProperties) {
        this.replicationProperties = replicationProperties;
    }

}
