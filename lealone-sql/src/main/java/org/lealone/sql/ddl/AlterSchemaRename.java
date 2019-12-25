/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

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
        Database db = session.getDatabase();
        synchronized (db.getLock(DbObjectType.SCHEMA)) {
            if (!oldSchema.canDrop()) {
                throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, oldSchema.getName());
            }
            if (db.findSchema(newSchemaName) != null || newSchemaName.equals(oldSchema.getName())) {
                throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, newSchemaName);
            }
            session.getUser().checkSchemaAdmin();
            db.renameDatabaseObject(session, oldSchema, newSchemaName);
            ArrayList<SchemaObject> all = db.getAllSchemaObjects();
            for (SchemaObject schemaObject : all) {
                db.updateMeta(session, schemaObject);
            }
        }
        return 0;
    }
}
