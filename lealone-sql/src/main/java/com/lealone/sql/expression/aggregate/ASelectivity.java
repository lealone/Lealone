/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.aggregate;

import com.lealone.db.Constants;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.IntIntHashMap;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueInt;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.query.Select;

public class ASelectivity extends BuiltInAggregate {

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
    public String getSQL() {
        return getSQL("SELECTIVITY");
    }

    // 会忽略distinct
    // 返回(100 * distinctCount/rowCount)
    public class AggregateDataSelectivity extends AggregateData {

        private long count;
        private IntIntHashMap distinctHashes;
        private double m2;

        @Override
        public void add(ServerSession session, Value v) {
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
        Value getValue(ServerSession session) {
            if (distinctHashes == null)
                return ValueInt.get(Constants.SELECTIVITY_DEFAULT);
            m2 += distinctHashes.size();
            m2 = 100 * m2 / count;
            int s = (int) m2;
            s = s <= 0 ? 1 : s > 100 ? 100 : s;
            Value v = ValueInt.get(s);
            return v.convertTo(dataType);
        }
    }
}
