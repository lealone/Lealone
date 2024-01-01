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
import com.lealone.db.schema.Constant;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Expression;

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
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.CONSTANT, session);
        if (lock == null)
            return -1;

        // 当成功获得排它锁后，不管以下代码是正常还是异常返回都不需要在这里手工释放锁，
        // 排它锁会在事务提交或回滚时自动被释放。
        if (schema.findConstant(session, constantName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.CONSTANT_ALREADY_EXISTS_1, constantName);
        }
        int id = getObjectId();
        expression = expression.optimize(session);
        Value value = expression.getValue(session);
        Constant constant = new Constant(schema, id, constantName, value);
        schema.add(session, constant, lock);
        return 0;
    }
}
