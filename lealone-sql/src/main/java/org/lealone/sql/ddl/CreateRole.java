/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Role;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE ROLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateRole extends DefineStatement implements AuthStatement {

    private String roleName;
    private boolean ifNotExists;

    public CreateRole(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_ROLE;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setRoleName(String name) {
        this.roleName = name;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        synchronized (db.getAuthLock()) {
            if (db.findUser(roleName) != null) {
                throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, roleName);
            }
            if (db.findRole(roleName) != null) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, roleName);
            }
            int id = getObjectId();
            Role role = new Role(db, id, roleName, false);
            db.addDatabaseObject(session, role);
        }
        return 0;
    }

}
