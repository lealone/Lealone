/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.db.Database;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataCount extends AggregateData {
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