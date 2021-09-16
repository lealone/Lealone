/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.ValueVector;

// COUNT(*)
public class ACountAll extends Aggregate {

    public ACountAll(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
        // 在Parser.readAggregate那里确保使用COUNT_ALL时distinct是false
        if (distinct) {
            throw DbException.getInternalError();
        }
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        dataType = Value.LONG;
        scale = 0;
        precision = ValueLong.PRECISION;
        displaySize = ValueLong.DISPLAY_SIZE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataCountAll();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "COUNT(*)";
    }

    private class AggregateDataCountAll extends AggregateData {

        private long count;

        @Override
        void add(Database database, Value v) {
            count++;
        }

        @Override
        void add(Database database, ValueVector bvv, ValueVector vv) {
            count += select.getTopTableFilter().getBatchSize();
        }

        @Override
        Value getValue(Database database) {
            Value v = ValueLong.get(count);
            return v.convertTo(dataType);
        }

        @Override
        void merge(Database database, Value v) {
            count += v.getLong();
        }

        @Override
        Value getMergedValue(Database database) {
            return ValueLong.get(count);
        }
    }
}
