/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

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
        Database db = session.getDatabase();
        if (table != null) {
            session.getUser().checkRight(table, Right.ALL);
        }
        if (cycle != null) {
            sequence.setCycle(cycle);
        }
        if (cacheSize != null) {
            long size = cacheSize.optimize(session).getValue(session).getLong();
            sequence.setCacheSize(size);
        }
        if (start != null || minValue != null || maxValue != null || increment != null) {
            Long startValue = getLong(start);
            Long min = getLong(minValue);
            Long max = getLong(maxValue);
            Long inc = getLong(increment);
            sequence.modify(startValue, min, max, inc);
        }
        // need to use the system session, so that the update
        // can be committed immediately - not committing it
        // would keep other transactions from using the sequence
        ServerSession sysSession = db.getSystemSession();
        synchronized (sysSession) {
            synchronized (db) {
                db.updateMeta(sysSession, sequence);
                sysSession.commit();
            }
        }
        return 0;
    }

    private Long getLong(Expression expr) {
        if (expr == null) {
            return null;
        }
        return expr.optimize(session).getValue(session).getLong();
    }

}
