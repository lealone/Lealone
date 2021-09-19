/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import java.util.Arrays;
import java.util.Comparator;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueLong;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.Select;

public class AHistogram extends BuiltInAggregate {

    public AHistogram(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        dataType = Value.ARRAY;
        scale = 0;
        precision = displaySize = Integer.MAX_VALUE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataHistogram();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return getSQL("HISTOGRAM", isDistributed);
    }

    // 会忽略distinct
    // 计算每个值出现的次数
    private class AggregateDataHistogram extends AggregateData {

        private long count;
        private ValueHashMap<AggregateDataHistogram> distinctValues;

        @Override
        void add(ServerSession session, Value v) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            AggregateDataHistogram a = distinctValues.get(v);
            if (a == null) {
                if (distinctValues.size() < Constants.SELECTIVITY_DISTINCT_COUNT) {
                    a = new AggregateDataHistogram();
                    distinctValues.put(v, a);
                }
            }
            if (a != null) {
                a.count++;
            }
        }

        @Override
        Value getValue(ServerSession session) {
            ValueArray[] values = new ValueArray[distinctValues.size()];
            int i = 0;
            for (Value dv : distinctValues.keys()) {
                AggregateDataHistogram d = distinctValues.get(dv);
                values[i] = ValueArray.get(new Value[] { dv, ValueLong.get(d.count) });
                i++;
            }
            final CompareMode compareMode = session.getDatabase().getCompareMode();
            Arrays.sort(values, new Comparator<ValueArray>() {
                @Override
                public int compare(ValueArray v1, ValueArray v2) {
                    Value a1 = v1.getList()[0];
                    Value a2 = v2.getList()[0];
                    return a1.compareTo(a2, compareMode);
                }
            });
            Value v = ValueArray.get(values);
            return v.convertTo(dataType);
        }

        @Override
        void merge(ServerSession session, Value v) {
            throw DbException.getUnsupportedException("merge");
        }

        @Override
        Value getMergedValue(ServerSession session) {
            throw DbException.getUnsupportedException("getMergedValue");
        }
    }
}
