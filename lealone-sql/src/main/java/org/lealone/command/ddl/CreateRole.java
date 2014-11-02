/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.Role;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * CREATE ROLE
 */
public class CreateRole extends DefineCommand {

    private String roleName;
    private boolean ifNotExists;

    public CreateRole(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setRoleName(String name) {
        this.roleName = name;
    }

    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
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
        return 0;
    }

    public int getType() {
        return CommandInterface.CREATE_ROLE;
    }

}
