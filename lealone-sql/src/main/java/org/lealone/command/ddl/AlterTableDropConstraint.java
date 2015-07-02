/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.constraint.Constraint;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaCommand {

    private String constraintName;
    private final boolean ifExists;

    public AlterTableDropConstraint(Session session, Schema schema, boolean ifExists) {
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
        return CommandInterface.ALTER_TABLE_DROP_CONSTRAINT;
    }

}
