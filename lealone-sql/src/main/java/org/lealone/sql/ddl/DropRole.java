/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Role;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP ROLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropRole extends DefinitionStatement implements AuthStatement {

    private String roleName;
    private boolean ifExists;

    public DropRole(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_ROLE;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public boolean isIfDDL() {
        return ifExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveAuthLock(session);
        if (lock == null)
            return -1;

        if (roleName.equals(Constants.PUBLIC_ROLE_NAME)) {
            throw DbException.get(ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1, roleName);
        }
        Role role = db.findRole(session, roleName);
        if (role == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, roleName);
            }
        } else {
            db.removeDatabaseObject(session, role, lock);
        }
        return 0;
    }
}
