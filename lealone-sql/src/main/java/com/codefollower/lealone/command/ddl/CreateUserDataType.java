/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.UserDataType;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.value.DataType;

/**
 * This class represents the statement
 * CREATE DOMAIN
 */
public class CreateUserDataType extends DefineCommand {

    private String typeName;
    private Column column;
    private boolean ifNotExists;

    public CreateUserDataType(Session session) {
        super(session);
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

    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        if (db.findUserDataType(typeName) != null) {
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
                throw DbException.get(ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1, typeName + " (" + table.getSQL() + ")");
            }
        }
        int id = getObjectId();
        UserDataType type = new UserDataType(db, id, typeName);
        type.setColumn(column);
        db.addDatabaseObject(session, type);
        return 0;
    }

    public int getType() {
        return CommandInterface.CREATE_DOMAIN;
    }

}
