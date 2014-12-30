/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.result;

import org.lealone.dbobject.table.Column;
import org.lealone.engine.Constants;
import org.lealone.value.Value;

/**
 * A simple row that contains data for only one column.
 */
public class SimpleRowValue implements SearchRow {

    private long key;
    private int version;
    private int index;
    private final int virtualColumnCount;
    private Value data;
    private Value rowKey;

    public SimpleRowValue(int columnCount) {
        this.virtualColumnCount = columnCount;
    }

    @Override
    public void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
        version = row.getVersion();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public int getColumnCount() {
        return virtualColumnCount;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public Value getValue(int idx) {
        return idx == index ? data : null;
    }

    @Override
    public void setValue(int idx, Value v) {
        setValue(idx, v, null);
    }

    @Override
    public void setValue(int idx, Value v, Column c) {
        if (c != null && c.isRowKeyColumn())
            this.rowKey = v;
        index = idx;
        data = v;
    }

    @Override
    public String toString() {
        return "( /* " + key + " */ " + (data == null ? "null" : data.getTraceSQL()) + " )";
    }

    @Override
    public int getMemory() {
        return Constants.MEMORY_OBJECT + (data == null ? 0 : data.getMemory());
    }

    @Override
    public void setRowKey(Value rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public Value getRowKey() {
        return rowKey;
    }

}
