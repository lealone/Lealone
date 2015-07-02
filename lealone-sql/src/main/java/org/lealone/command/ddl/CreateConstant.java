/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Constant;
import org.lealone.dbobject.Schema;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.expression.Expression;
import org.lealone.message.DbException;
import org.lealone.value.Value;

/**
 * This class represents the statement
 * CREATE CONSTANT
 */
public class CreateConstant extends SchemaCommand {

    private String constantName;
    private Expression expression;
    private boolean ifNotExists;

    public CreateConstant(Session session, Schema schema) {
        super(session, schema);
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
        Constant constant = new Constant(getSchema(), id, constantName);
        expression = expression.optimize(session);
        Value value = expression.getValue(session);
        constant.setValue(value);
        db.addSchemaObject(session, constant);
        return 0;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    public void setExpression(Expression expr) {
        this.expression = expr;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_CONSTANT;
    }

}
