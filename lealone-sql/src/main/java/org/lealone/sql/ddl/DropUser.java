/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Auth;
import org.lealone.db.auth.User;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP USER
 */
public class DropUser extends DefineStatement {

    private boolean ifExists;
    private String userName;

    public DropUser(ServerSession session) {
        super(session);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = LealoneDatabase.getInstance();
        User user = Auth.findUser(userName);
        if (user == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.USER_NOT_FOUND_1, userName);
            }
        } else {
            if (user == session.getUser()) {
                int adminUserCount = 0;
                for (User u : Auth.getAllUsers()) {
                    if (u.isAdmin()) {
                        adminUserCount++;
                    }
                }
                if (adminUserCount == 1) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_CURRENT_USER);
                }
            }
            user.checkOwnsNoSchemas();
            db.removeDatabaseObject(session, user);
        }
        return 0;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_USER;
    }

}
