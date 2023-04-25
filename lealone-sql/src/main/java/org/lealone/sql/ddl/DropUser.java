/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP USER
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropUser extends AuthStatement {

    private String userName;
    private boolean ifExists;

    public DropUser(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_USER;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveAuthLock(session);
        if (lock == null)
            return -1;

        User user = db.findUser(session, userName);
        if (user == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.USER_NOT_FOUND_1, userName);
            }
        } else {
            if (user == session.getUser()) {
                int adminUserCount = 0;
                for (User u : db.getAllUsers()) {
                    if (u.isAdmin()) {
                        adminUserCount++;
                    }
                }
                // 运行到这里时当前用户必定是有Admin权限的，如果当前用户想删除它自己，
                // 同时系统中又没有其他Admin权限的用户了，那么不允许它删除自己
                if (adminUserCount == 1) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_CURRENT_USER);
                }
            }
            user.checkOwnsNoSchemas(session); // 删除用户前需要删除它拥有的所有Schema，否则不允许删
            db.removeDatabaseObject(session, user, lock);
        }
        return 0;
    }
}
