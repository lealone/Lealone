/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.Sequence;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * ALTER SEQUENCE
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterSequence extends SchemaStatement {

    private Table table;
    private Sequence sequence;
    private Expression start;
    private Expression increment;
    private Boolean cycle;
    private Expression minValue;
    private Expression maxValue;
    private Expression cacheSize;

    public AlterSequence(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_SEQUENCE;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public void setColumn(Column column) {
        table = column.getTable();
        sequence = column.getSequence();
        if (sequence == null) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, column.getSQL());
        }
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    @Override
    public int update() {
        if (table != null) {
            session.getUser().checkRight(table, Right.ALL);
        }
        sequence.tryLock(session, false);
        Sequence newSequence = sequence.copy();
        if (cycle != null) {
            newSequence.setCycle(cycle);
        }
        if (cacheSize != null) {
            long size = cacheSize.optimize(session).getValue(session).getLong();
            newSequence.setCacheSize(size);
        }
        if (start != null || minValue != null || maxValue != null || increment != null) {
            Long startValue = getLong(start);
            Long min = getLong(minValue);
            Long max = getLong(maxValue);
            Long inc = getLong(increment);
            newSequence.modify(startValue, min, max, inc);
        }
        newSequence.setOldSequence(sequence);
        schema.update(session, newSequence, sequence.getOldRow(), sequence.getLock());
        return 0;
    }

    private Long getLong(Expression expr) {
        return CreateSequence.getLong(session, expr);
    }
}
