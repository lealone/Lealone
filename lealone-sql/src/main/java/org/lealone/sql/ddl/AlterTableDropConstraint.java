/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.schema.Schema;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaStatement {

    private String constraintName;
    private final boolean ifExists;

    public AlterTableDropConstraint(ServerSession session, Schema schema, boolean ifExists) {
        super(session, schema);
        this.ifExists = ifExists;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    @Override
    public int update() {
        session.commit(true);
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
            }
        } else {
            session.getUser().checkRight(constraint.getTable(), Right.ALL);
            session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
            session.getDatabase().removeSchemaObject(session, constraint);
        }
        return 0;
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_TABLE_DROP_CONSTRAINT;
    }

}
