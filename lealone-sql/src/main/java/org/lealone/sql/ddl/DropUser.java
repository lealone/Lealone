/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
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
                // 运行到这里时当前用户必定是有Admin权限的，如果当前用户想删除它自己，
                // 同时系统中又没当其他Admin权限的用户了，那么不允许它删除自己
                if (adminUserCount == 1) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_CURRENT_USER);
                }
            }
            user.checkOwnsNoSchemas(session); // 删除用户前需要删除它拥有的所有Schema，否则不允许删
            LealoneDatabase.getInstance().removeDatabaseObject(session, user);
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
