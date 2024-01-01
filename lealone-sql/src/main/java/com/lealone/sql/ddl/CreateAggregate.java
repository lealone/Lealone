/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.UserAggregate;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

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
