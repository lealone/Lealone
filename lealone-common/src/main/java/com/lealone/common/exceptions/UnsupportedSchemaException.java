/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.exceptions;

import com.lealone.db.session.Session;

public class UnsupportedSchemaException extends RuntimeException {

    // public static final UnsupportedSchemaException INSTANCE = new UnsupportedSchemaException();

    private String sql;
    private Session session;

    public UnsupportedSchemaException() {
    }

    public UnsupportedSchemaException(String msg) {
        super(msg);
    }

    public UnsupportedSchemaException(Session session, String sql) {
        this.session = session;
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

}
