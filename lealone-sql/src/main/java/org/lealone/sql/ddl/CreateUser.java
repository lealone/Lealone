/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.security.SHA256;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.User;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE USER
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateUser extends DefineStatement implements AuthStatement {

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

    @Override
    public int getType() {
        return SQLStatement.CREATE_USER;
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
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        synchronized (db.getAuthLock()) {
            if (db.findRole(userName) != null) {
                throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
            }
            if (db.findUser(userName) != null) {
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
                throw DbException.throwInternalError();
            }
            db.addDatabaseObject(session, user);
        }
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
        char[] passwordChars = pwd == null ? new char[0] : pwd.toCharArray();
        byte[] userPasswordHash;
        String userName = user.getName();
        // 不能用用户名和密码组成hash，否则重命名用户后将不能通过原来的密码登录
        // TODO 如果不用固定的名称是否还有更好办法？
        userName = Constants.PROJECT_NAME;
        if (userName.isEmpty() && passwordChars.length == 0) {
            userPasswordHash = new byte[0];
        } else {
            userPasswordHash = SHA256.getKeyPasswordHash(userName, passwordChars);
        }
        user.setUserPasswordHash(userPasswordHash);
    }

}
