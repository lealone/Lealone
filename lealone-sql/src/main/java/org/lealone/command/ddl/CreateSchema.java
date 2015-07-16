/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.User;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * CREATE SCHEMA
 */
public class CreateSchema extends DefineCommand {

    private String schemaName;
    private String authorization;
    private boolean ifNotExists;

    private Map<String, String> replicationProperties;

    public CreateSchema(Session session) {
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
        User user = db.getUser(authorization);
        // during DB startup, the Right/Role records have not yet been loaded
        if (!db.isStarting()) {
            user.checkSchemaAdmin();
        }
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
        return CommandInterface.CREATE_SCHEMA;
    }

    public void setReplicationProperties(Map<String, String> replicationProperties) {
        this.replicationProperties = replicationProperties;
    }

}
