/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.SchemaObject;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER SCHEMA RENAME
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterSchemaRename extends DefinitionStatement {

    private Schema oldSchema;
    private String newSchemaName;

    public AlterSchemaRename(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_SCHEMA_RENAME;
    }

    public void setOldSchema(Schema schema) {
        oldSchema = schema;
    }

    public void setNewName(String name) {
        newSchemaName = name;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveSchemaLock(session);
        if (lock == null)
            return -1;

        if (!oldSchema.canDrop()) {
            throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, oldSchema.getName());
        }
        if (db.findSchema(session, newSchemaName) != null || newSchemaName.equals(oldSchema.getName())) {
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, newSchemaName);
        }
        db.renameDatabaseObject(session, oldSchema, newSchemaName, lock);
        ArrayList<SchemaObject> all = db.getAllSchemaObjects();
        for (SchemaObject schemaObject : all) {
            db.updateMeta(session, schemaObject);
        }
        return 0;
    }
}
