/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction;

/**
 * A versioned value (possibly null). 
 * It contains a pointer to the old value, and the value itself.
 * 
 * @author H2 Group
 * @author zhh
 */
class VersionedValue {

    public long tid;

    public int logId;

    /**
     * The value.
     */
    public Object value;

    public VersionedValue() {
    }

    public VersionedValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("VersionedValue[ value = ").append(value);
        buff.append(", tid = ").append(tid).append(", logId = ").append(logId).append(" ]");
        return buff.toString();
    }
}
