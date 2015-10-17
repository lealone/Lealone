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
import org.lealone.db.ServerSession;
import org.lealone.db.UserAggregate;
import org.lealone.db.schema.Schema;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE AGGREGATE
 */
public class CreateAggregate extends DefineStatement {

    private Schema schema;
    private String name;
    private String javaClassName;
    private boolean ifNotExists;
    private boolean force;

    public CreateAggregate(ServerSession session) {
        super(session);
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (db.findAggregate(name) != null || schema.findFunction(name) != null) {
            if (!ifNotExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
            }
        } else {
            int id = getObjectId();
            UserAggregate aggregate = new UserAggregate(db, id, name, javaClassName, force);
            db.addDatabaseObject(session, aggregate);
        }
        return 0;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setJavaClassName(String string) {
        this.javaClassName = string;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_AGGREGATE;
    }

}
