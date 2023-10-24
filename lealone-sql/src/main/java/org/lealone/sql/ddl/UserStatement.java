/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Expression;

public abstract class UserStatement extends AuthStatement {

    protected Expression password;
    protected Expression salt;
    protected Expression hash;
    protected Expression saltMongo;
    protected Expression hashMongo;
    protected Expression saltMySQL;
    protected Expression hashMySQL;
    protected Expression saltPostgreSQL;
    protected Expression hashPostgreSQL;

    protected UserStatement(ServerSession session) {
        super(session);
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

    public void setSaltMongo(Expression e) {
        saltMongo = e;
    }

    public void setHashMongo(Expression e) {
        hashMongo = e;
    }

    public void setSaltMySQL(Expression e) {
        saltMySQL = e;
    }

    public void setHashMySQL(Expression e) {
        hashMySQL = e;
    }

    public void setSaltPostgreSQL(Expression e) {
        saltPostgreSQL = e;
    }

    public void setHashPostgreSQL(Expression e) {
        hashPostgreSQL = e;
    }
}
