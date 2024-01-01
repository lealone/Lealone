/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Role;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE ROLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateRole extends AuthStatement {

    private String roleName;
    private boolean ifNotExists;

    public CreateRole(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_ROLE;
    }

    public void setRoleName(String name) {
        this.roleName = name;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveAuthLock(session);
        if (lock == null)
            return -1;

        if (db.findUser(session, roleName) != null) {
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, roleName);
        }
        if (db.findRole(session, roleName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, roleName);
        }
        int id = getObjectId();
        Role role = new Role(db, id, roleName, false);
        db.addDatabaseObject(session, role, lock);
        return 0;
    }
}
