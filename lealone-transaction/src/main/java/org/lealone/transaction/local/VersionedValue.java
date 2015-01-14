/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.local;

/**
 * A versioned value (possibly null). It contains a pointer to the old
 * value, and the value itself.
 */
class VersionedValue {

    /**
     * The operation id.
     */
    public long operationId;

    /**
     * The value.
     */
    public Object value;

    @Override
    public String toString() {
        return value + (operationId == 0 ? "" : //
                (" " + DefaultTransactionEngine.getTransactionId(operationId) + "/" + DefaultTransactionEngine.getLogId(operationId)));
    }

}