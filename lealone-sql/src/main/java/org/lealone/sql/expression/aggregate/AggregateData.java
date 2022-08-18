/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;

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
     * @param session the session
     * @param v the value
     */
    abstract void add(ServerSession session, Value v);

    /**
     * Get the aggregate result.
     *
     * @param session the session
     * @return the value
     */
    abstract Value getValue(ServerSession session);

    abstract void merge(ServerSession session, Value v);

    abstract Value getMergedValue(ServerSession session);
}
