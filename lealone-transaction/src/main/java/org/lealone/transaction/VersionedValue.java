/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction;

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
        StringBuilder buff = new StringBuilder();
        buff.append("VersionedValue[ value = ").append(value);
        buff.append(", operationId = ").append(operationId);

        if (operationId != 0) {
            buff.append(", transactionId = ").append(DefaultTransactionEngine.getTransactionId(operationId));
            buff.append(", logId = ").append(DefaultTransactionEngine.getLogId(operationId));
        }

        buff.append("]");
        return buff.toString();
    }
}
