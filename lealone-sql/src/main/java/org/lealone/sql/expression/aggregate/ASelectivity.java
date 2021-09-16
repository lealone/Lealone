/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.IntIntHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.Select;

public class ASelectivity extends Aggregate {

    public ASelectivity(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        dataType = Value.INT;
        scale = 0;
        precision = ValueInt.PRECISION;
        displaySize = ValueInt.DISPLAY_SIZE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataSelectivity();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return getSQL("SELECTIVITY", isDistributed);
    }

    // 会忽略distinct
    // 返回(100 * distinctCount/rowCount)
    private class AggregateDataSelectivity extends AggregateData {

        private long count;
        private IntIntHashMap distinctHashes;
        private double m2;
        private Value value;

        @Override
        void add(Database database, Value v) {
            // 是基于某个表达式(多数是单个字段)算不重复的记录数所占总记录数的百分比
            // Constants.SELECTIVITY_DISTINCT_COUNT默认是1万，这个值不能改，
            // 对统计值影响很大。通常这个值越大，统计越精确，但是会使用更多内存。
            // SELECTIVITY越大，说明重复的记录越少，在选择索引时更有利。
            count++;
            if (distinctHashes == null) {
                distinctHashes = new IntIntHashMap();
            }
            int size = distinctHashes.size();
            if (size > Constants.SELECTIVITY_DISTINCT_COUNT) {
                distinctHashes = new IntIntHashMap();
                m2 += size;
            }
            int hash = v.hashCode();
            // the value -1 is not supported
            distinctHashes.put(hash, 1);
        }

        @Override
        Value getValue(Database database) {
            m2 += distinctHashes.size();
            m2 = 100 * m2 / count;
            int s = (int) m2;
            s = s <= 0 ? 1 : s > 100 ? 100 : s;
            Value v = ValueInt.get(s);
            return v.convertTo(dataType);
        }

        @Override
        void merge(Database database, Value v) {
            count++;
            if (value == null) {
                value = v.convertTo(dataType);
            } else {
                v = v.convertTo(value.getType());
                value = value.add(v);
            }
        }

        @Override
        Value getMergedValue(Database database) {
            Value v = null;
            if (value != null) {
                v = Aggregate.divide(value, count);
            }
            return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
        }
    }
}
