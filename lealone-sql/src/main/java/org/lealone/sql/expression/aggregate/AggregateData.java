/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.db.Database;
import org.lealone.db.value.Value;
import org.lealone.sql.vector.ValueVector;

/**
 * Abstract class for the computation of an aggregate.
 * 
 * @author H2 Group
 * @author zhh
 */
abstract class AggregateData {

    /**
     * Add a value to this aggregate.
     *
     * @param database the database
     * @param v the value
     */
    abstract void add(Database database, Value v);

    void add(Database database, ValueVector bvv, ValueVector vv) {
    }

    /**
     * Get the aggregate result.
     *
     * @param database the database
     * @return the value
     */
    abstract Value getValue(Database database);

    abstract void merge(Database database, Value v);

    abstract Value getMergedValue(Database database);
}
