/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

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

    @Override
    public int update() {
        Database db = session.getDatabase();
        synchronized (getSchema().getLock(DbObjectType.SEQUENCE)) {
            if (getSchema().findSequence(sequenceName) != null) {
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
            Sequence sequence = new Sequence(getSchema(), id, sequenceName, startValue, inc, cache, min, max, cycle,
                    belongsToTable);
            db.addSchemaObject(session, sequence);
        }
        return 0;
    }

    private Long getLong(Expression expr) {
        if (expr == null) {
            return null;
        }
        return expr.optimize(session).getValue(session).getLong();
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

}
