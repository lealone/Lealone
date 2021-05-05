/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.UserAggregate;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE AGGREGATE
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateAggregate extends SchemaStatement {

    private String name;
    private String javaClassName;
    private boolean ifNotExists;
    private boolean force;

    public CreateAggregate(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_AGGREGATE;
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

    @Override
    public boolean isIfDDL() {
        return ifNotExists;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.AGGREGATE, session);
        if (lock == null)
            return -1;

        if (schema.findAggregate(session, name) != null || schema.findFunction(session, name) != null) {
            if (!ifNotExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
            }
        } else {
            int id = getObjectId();
            UserAggregate aggregate = new UserAggregate(schema, id, name, javaClassName, force);
            schema.add(session, aggregate, lock);
        }
        return 0;
    }
}
