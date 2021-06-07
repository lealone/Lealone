/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import org.lealone.common.trace.TraceObjectType;
import org.lealone.db.value.ArrayBase;
import org.lealone.db.value.Value;

/**
 * Represents an ARRAY value.
 */
public class JdbcArray extends ArrayBase {

    private final JdbcConnection conn;

    /**
     * INTERNAL
     */
    JdbcArray(JdbcConnection conn, Value value, int id) {
        this.conn = conn;
        this.value = value;
        this.trace = conn.getTrace(TraceObjectType.ARRAY, id);
    }

    @Override
    protected void checkClosed() {
        conn.checkClosed();
        super.checkClosed();
    }
}
