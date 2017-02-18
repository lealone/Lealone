/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.UserDataType;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP DOMAIN
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropUserDataType extends DefineStatement {

    private String typeName;
    private boolean ifExists;

    public DropUserDataType(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_DOMAIN;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        synchronized (db.getLock(DbObjectType.USER_DATATYPE)) {
            UserDataType type = db.findUserDataType(typeName);
            if (type == null) {
                if (!ifExists) {
                    throw DbException.get(ErrorCode.USER_DATA_TYPE_NOT_FOUND_1, typeName);
                }
            } else {
                db.removeDatabaseObject(session, type);
            }
        }
        return 0;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

}
