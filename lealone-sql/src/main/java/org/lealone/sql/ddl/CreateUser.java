/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.security.SHA256;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.User;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE USER
 */
public class CreateUser extends DefineStatement {

    private String userName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean ifNotExists;
    private String comment;

    public CreateUser(ServerSession session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(Expression password) {
        this.password = password;
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
        char[] passwordChars = pwd == null ? new char[0] : pwd.toCharArray();
        byte[] userPasswordHash;
        String userName = user.getName();
        if (userName.length() == 0 && passwordChars.length == 0) {
            userPasswordHash = new byte[0];
        } else {
            userPasswordHash = SHA256.getKeyPasswordHash(userName, passwordChars);
        }
        user.setUserPasswordHash(userPasswordHash);
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = LealoneDatabase.getInstance();
        if (db.findRole(userName) != null) {
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
        }
        if (db.findUser(userName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId(db);
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            setSaltAndHash(user, session, salt, hash);
        } else if (password != null) {
            setPassword(user, session, password);
        } else {
            throw DbException.throwInternalError();
        }
        db.addDatabaseObject(session, user);
        return 0;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_USER;
    }

}
