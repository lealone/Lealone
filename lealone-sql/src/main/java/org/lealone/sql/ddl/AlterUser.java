/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statements
 * ALTER USER ADMIN,
 * ALTER USER RENAME,
 * ALTER USER SET PASSWORD
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterUser extends DefinitionStatement implements AuthStatement {

    private int type;
    private User user;
    private String newName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;

    public AlterUser(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    @Override
    public int update() {
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveAuthLock(session);
        if (lock == null)
            return -1;

        switch (type) {
        case SQLStatement.ALTER_USER_SET_PASSWORD:
            if (user != session.getUser()) {
                session.getUser().checkAdmin();
            }
            if (hash != null && salt != null) {
                CreateUser.setSaltAndHash(user, session, salt, hash);
            } else {
                CreateUser.setPassword(user, session, password);
            }
            db.updateMeta(session, user);
            break;
        case SQLStatement.ALTER_USER_RENAME:
            session.getUser().checkAdmin();
            if (db.findUser(session, newName) != null || newName.equals(user.getName())) {
                throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, newName);
            }
            db.renameDatabaseObject(session, user, newName, lock);
            break;
        case SQLStatement.ALTER_USER_ADMIN:
            session.getUser().checkAdmin();
            if (!admin) {
                user.checkOwnsNoSchemas(session);
            }
            user.setAdmin(admin);
            db.updateMeta(session, user);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }
}
