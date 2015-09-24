/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.CommandInterface;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.Role;
import org.lealone.db.Session;

/**
 * This class represents the statement
 * DROP ROLE
 */
public class DropRole extends DefineCommand {

    private String roleName;
    private boolean ifExists;

    public DropRole(Session session) {
        super(session);
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (roleName.equals(Constants.PUBLIC_ROLE_NAME)) {
            throw DbException.get(ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1, roleName);
        }
        Role role = db.findRole(roleName);
        if (role == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, roleName);
            }
        } else {
            db.removeDatabaseObject(session, role);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ROLE;
    }

}
