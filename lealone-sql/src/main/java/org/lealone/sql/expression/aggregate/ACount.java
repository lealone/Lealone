/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.ValueVector;

// COUNT(x)
public class ACount extends Aggregate {

    public ACount(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
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
        return new AggregateDataCount();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return getSQL("COUNT", isDistributed);
    }

    private static class AggregateDataCount extends AggregateData {

        private long count;
        private ValueHashMap<AggregateDataCount> distinctValues;

        @Override
        void add(Database database, int dataType, boolean distinct, Value v) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            count++;
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
                return;
            }
        }

        @Override
        void add(Database database, int dataType, boolean distinct, ValueVector bvv, ValueVector vv) {
            count += vv.size();
        }

        @Override
        Value getValue(Database database, int dataType, boolean distinct) {
            if (distinct) {
                if (distinctValues != null) {
                    count = distinctValues.size();
                } else {
                    count = 0;
                }
            }
            Value v = ValueLong.get(count);
            return v.convertTo(dataType);
        }

        @Override
        void merge(Database database, int dataType, boolean distinct, Value v) {
            count += v.getLong();
        }

        @Override
        Value getMergedValue(Database database, int dataType, boolean distinct) {
            return ValueLong.get(count);
        }
    }
}
