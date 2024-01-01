/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.constraint.Constraint;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterTableDropConstraint extends SchemaStatement {

    private String constraintName;
    private final boolean ifExists;

    public AlterTableDropConstraint(ServerSession session, Schema schema, boolean ifExists) {
        super(session, schema);
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_TABLE_DROP_CONSTRAINT;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.CONSTRAINT, session);
        if (lock == null)
            return -1;

        Constraint constraint = schema.findConstraint(session, constraintName);
        if (constraint == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
            }
        } else {
            session.getUser().checkRight(constraint.getTable(), Right.ALL);
            session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
            schema.remove(session, constraint, lock);
        }
        return 0;
    }
}
