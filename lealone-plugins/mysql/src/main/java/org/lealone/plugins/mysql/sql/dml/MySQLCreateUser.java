/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.dml;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mysql.db.MySQLUser;
import org.lealone.plugins.mysql.server.util.SecurityUtil;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.ddl.AuthStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE USER
 * 
 * @author H2 Group
 * @author zhh
 */
public class MySQLCreateUser extends AuthStatement {

    private String userName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private String comment;
    private boolean ifNotExists;

    public MySQLCreateUser(ServerSession session) {
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

    public void setPassword(Expression password) {
        this.password = password;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
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
        MySQLUser user = new MySQLUser(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            setSaltAndHash(user, session, salt, hash);
        } else if (password != null) {
            setPassword(user, session, password);
        } else {
            throw DbException.getInternalError();
        }
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
        if (pwd == null || pwd.isEmpty()) {
            user.setSaltAndHash(new byte[0], new byte[0]);
            return;
        }
        byte[] hash = SecurityUtil.sha1(pwd);
        user.setSaltAndHash(new byte[0], hash);
    }
}
