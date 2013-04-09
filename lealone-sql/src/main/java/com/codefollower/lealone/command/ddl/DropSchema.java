/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;

/**
 * This class represents the statement
 * DROP SCHEMA
 */
public class DropSchema extends DefineCommand {

    private String schemaName;
    private boolean ifExists;

    public DropSchema(Session session) {
        super(session);
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = db.findSchema(schemaName);
        if (schema == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
            }
        } else {
            if (!schema.canDrop()) {
                throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, schemaName);
            }
            db.removeDatabaseObject(session, schema);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public int getType() {
        return CommandInterface.DROP_SCHEMA;
    }

}
