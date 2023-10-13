/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE SCHEMA
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateSchema extends DefinitionStatement {

    private String schemaName;
    private String authorization;
    private boolean ifNotExists;

    public CreateSchema(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_SCHEMA;
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    public void setAuthorization(String userName) {
        this.authorization = userName;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveSchemaLock(session);
        if (lock == null)
            return -1;

        User user = db.getUser(session, authorization);
        // during DB startup, the Right/Role records have not yet been loaded
        if (!db.isStarting()) {
            user.checkSchemaAdmin();
        }
        if (db.findSchema(session, schemaName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, schemaName);
        }
        int id = getObjectId();
        Schema schema = new Schema(db, id, schemaName, user, false);
        db.addDatabaseObject(session, schema, lock);
        return 0;
    }
}
