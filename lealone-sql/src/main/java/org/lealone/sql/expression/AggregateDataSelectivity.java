/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.common.value.Value;
import org.lealone.common.value.ValueInt;
import org.lealone.common.value.ValueNull;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.util.IntIntHashMap;

/**
 * Data stored while calculating a SELECTIVITY aggregate.
 */
class AggregateDataSelectivity extends AggregateData {
    private long count;
    private IntIntHashMap distinctHashes;
    private double m2;
    private Value value;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        // 是基于某个表达式(多数是单个字段)算不重复的记录数所占总记录数的百分比
        // org.h2.engine.Constants.SELECTIVITY_DISTINCT_COUNT默认是1万，这个值不能改，
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
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
        }
        Value v = null;
        int s = 0;
        if (count == 0) {
            s = 0;
        } else {
            m2 += distinctHashes.size();
            m2 = 100 * m2 / count;
            s = (int) m2;
            s = s <= 0 ? 1 : s > 100 ? 100 : s;
        }
        v = ValueInt.get(s);
        return v.convertTo(dataType);
    }

    @Override
    void merge(Database database, int dataType, boolean distinct, Value v) {
        count++;
        if (value == null) {
            value = v.convertTo(dataType);
        } else {
            v = v.convertTo(value.getType());
            value = value.add(v);
        }
    }

    @Override
    Value getMergedValue(Database database, int dataType, boolean distinct) {
        Value v = null;
        if (value != null) {
            v = Aggregate.divide(value, count);
        }
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }
}