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
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.UserAggregate;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP AGGREGATE
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropAggregate extends SchemaStatement {

    private String name;
    private boolean ifExists;

    public DropAggregate(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_AGGREGATE;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        synchronized (schema.getLock(DbObjectType.AGGREGATE)) {
            UserAggregate aggregate = schema.findAggregate(name);
            if (aggregate == null) {
                if (!ifExists) {
                    throw DbException.get(ErrorCode.AGGREGATE_NOT_FOUND_1, name);
                }
            } else {
                schema.remove(session, aggregate);
            }
        }
        return 0;
    }
}
