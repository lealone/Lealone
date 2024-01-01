/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Database;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.PasswordHash;
import com.lealone.db.auth.User;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE USER
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateUser extends UserStatement {

    private String userName;
    private boolean admin;
    private String comment;
    private boolean ifNotExists;

    public CreateUser(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_USER;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
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

        if (db.findRole(session, userName) != null) {
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
        }
        if (db.findUser(session, userName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId();
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            setSaltAndHash(user, session, salt, hash);
        } else if (password != null) {
            setPassword(user, session, password);
        } else {
            throw DbException.getInternalError();
        }
        if (hashMongo != null && saltMongo != null)
            user.setSaltAndHashMongo(getByteArray(session, saltMongo), getByteArray(session, hashMongo));
        if (hashMySQL != null && saltMySQL != null)
            user.setSaltAndHashMySQL(getByteArray(session, saltMySQL), getByteArray(session, hashMySQL));
        if (hashPostgreSQL != null && saltPostgreSQL != null)
            user.setSaltAndHashPostgreSQL(getByteArray(session, saltPostgreSQL),
                    getByteArray(session, hashPostgreSQL));
        db.addDatabaseObject(session, user, lock);
        return 0;
    }

    /**
     * Set the salt and hash for the given user.
     *
     * @param user the user
     * @param session the session
     * @param salt the salt
     * @param hash the hash
     */
    static void setSaltAndHash(User user, ServerSession session, Expression salt, Expression hash) {
        user.setSaltAndHash(getByteArray(session, salt), getByteArray(session, hash));
    }

    private static byte[] getByteArray(ServerSession session, Expression e) {
        String s = e.optimize(session).getValue(session).getString();
        return s == null ? new byte[0] : StringUtils.convertHexToBytes(s);
    }

    /**
     * Set the password for the given user.
     *
     * @param user the user
     * @param session the session
     * @param password the password
     */
    static void setPassword(User user, ServerSession session, Expression password) {
        String pwd = password.optimize(session).getValue(session).getString();
        PasswordHash.setPassword(user, pwd);
    }
}
