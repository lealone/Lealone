/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.SchemaObject;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;

import java.util.ArrayList;

/**
 * This class represents the statement
 * ALTER SCHEMA RENAME
 */
public class AlterSchemaRename extends DefineCommand {

    private Schema oldSchema;
    private String newSchemaName;

    public AlterSchemaRename(Session session) {
        super(session);
    }

    public void setOldSchema(Schema schema) {
        oldSchema = schema;
    }

    public void setNewName(String name) {
        newSchemaName = name;
    }

    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        if (!oldSchema.canDrop()) {
            throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, oldSchema.getName());
        }
        if (db.findSchema(newSchemaName) != null || newSchemaName.equals(oldSchema.getName())) {
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, newSchemaName);
        }
        session.getUser().checkAdmin();
        db.renameDatabaseObject(session, oldSchema, newSchemaName);
        ArrayList<SchemaObject> all = db.getAllSchemaObjects();
        for (SchemaObject schemaObject : all) {
            db.update(session, schemaObject);
        }
        return 0;
    }

    public int getType() {
        return CommandInterface.ALTER_SCHEMA_RENAME;
    }

}
