/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.schema.Constant;
import org.lealone.db.schema.Schema;
import org.lealone.db.value.Value;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE CONSTANT
 */
public class CreateConstant extends SchemaStatement {

    private String constantName;
    private Expression expression;
    private boolean ifNotExists;

    public CreateConstant(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_CONSTANT;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (getSchema().findConstant(constantName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.CONSTANT_ALREADY_EXISTS_1, constantName);
        }
        int id = getObjectId();
        expression = expression.optimize(session);
        Value value = expression.getValue(session);
        Constant constant = new Constant(getSchema(), id, constantName, value);
        db.addSchemaObject(session, constant);
        return 0;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    public void setExpression(Expression expr) {
        this.expression = expr;
    }

}
