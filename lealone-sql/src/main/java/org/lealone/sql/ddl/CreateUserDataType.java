/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.UserDataType;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.DataType;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE DOMAIN
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateUserDataType extends SchemaStatement {

    private String typeName;
    private Column column;
    private boolean ifNotExists;

    public CreateUserDataType(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DOMAIN;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        synchronized (getSchema().getLock(DbObjectType.USER_DATATYPE)) {
            if (getSchema().findUserDataType(typeName) != null) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1, typeName);
            }
            DataType builtIn = DataType.getTypeByName(typeName);
            if (builtIn != null) {
                if (!builtIn.hidden) {
                    throw DbException.get(ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1, typeName);
                }
                Table table = session.getDatabase().getFirstUserTable();
                if (table != null) {
                    throw DbException.get(ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1,
                            typeName + " (" + table.getSQL() + ")");
                }
            }
            int id = getObjectId();
            UserDataType type = new UserDataType(getSchema(), id, typeName);
            type.setColumn(column);
            db.addSchemaObject(session, type);
        }
        return 0;
    }

}
