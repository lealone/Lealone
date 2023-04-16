/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.result;

import org.lealone.db.Constants;
import org.lealone.db.value.Value;

/**
 * A simple row that contains data for only one column.
 */
public class SimpleRowValue extends RowBase {

    private final int virtualColumnCount;
    private int index;
    private Value data;

    public SimpleRowValue(int columnCount) {
        this.virtualColumnCount = columnCount;
    }

    @Override
    public int getColumnCount() {
        return virtualColumnCount;
    }

    @Override
    public Value getValue(int idx) {
        return idx == index ? data : null;
    }

    @Override
    public void setValue(int idx, Value v) {
        index = idx;
        data = v;
    }

    @Override
    public int getMemory() {
        if (memory == 0) {
            memory = Constants.MEMORY_OBJECT + (data == null ? 0 : data.getMemory());
        }
        return memory;
    }

    @Override
    public String toString() {
        return "( /* " + key + " */ " + (data == null ? "null" : data.getTraceSQL()) + " )";
    }
}
