/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Constant;
import org.lealone.db.schema.Schema;
import org.lealone.db.value.Value;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE CONSTANT
 * 
 * @author H2 Group
 * @author zhh
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

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    public void setExpression(Expression expr) {
        this.expression = expr;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        synchronized (getSchema().getLock(DbObjectType.CONSTANT)) {
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
            session.getDatabase().addSchemaObject(session, constant);
        }
        return 0;
    }

}
