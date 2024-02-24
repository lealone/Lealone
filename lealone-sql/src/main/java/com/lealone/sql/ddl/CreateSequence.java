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
import com.lealone.db.schema.Sequence;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CREATE SEQUENCE
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateSequence extends SchemaStatement {

    private String sequenceName;
    private boolean ifNotExists;
    private boolean cycle;
    private Expression minValue;
    private Expression maxValue;
    private Expression start;
    private Expression increment;
    private Expression cacheSize;
    private boolean belongsToTable;
    private boolean transactional;

    public CreateSequence(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_SEQUENCE;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.SEQUENCE, session);
        if (lock == null)
            return -1;

        if (schema.findSequence(session, sequenceName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SEQUENCE_ALREADY_EXISTS_1, sequenceName);
        }
        int id = getObjectId();
        Long startValue = getLong(start);
        Long inc = getLong(increment);
        Long cache = getLong(cacheSize);
        Long min = getLong(minValue);
        Long max = getLong(maxValue);
        Sequence sequence = new Sequence(schema, id, sequenceName, startValue, inc, cache, min, max,
                cycle, belongsToTable);
        sequence.setTransactional(transactional);
        schema.add(session, sequence, lock);
        return 0;
    }

    private Long getLong(Expression expr) {
        return getLong(session, expr);
    }

    static Long getLong(ServerSession session, Expression expr) {
        if (expr == null) {
            return null;
        }
        return expr.optimize(session).getValue(session).getLong();
    }
}
