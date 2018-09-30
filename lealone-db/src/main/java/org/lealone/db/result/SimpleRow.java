/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.result;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;

/**
 * Represents a simple row without state.
 */
public class SimpleRow implements SearchRow {

    private long key;
    private int version;
    private final Value[] data;
    private int memory;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    @Override
    public int getColumnCount() {
        return data.length;
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
    public void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
        version = row.getVersion();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setValue(int idx, Value v) {
        setValue(idx, v, null);
    }

    @Override
    public void setValue(int idx, Value v, Column c) {
        data[idx] = v;
    }

    @Override
    public Value getValue(int i) {
        return data[i];
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        buff.append(" */ ");
        for (Value v : data) {
            buff.appendExceptFirst(", ");
            buff.append(v == null ? "null" : v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public int getMemory() {
        if (memory == 0) {
            int len = data.length;
            memory = Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (int i = 0; i < len; i++) {
                Value v = data[i];
                if (v != null) {
                    memory += v.getMemory();
                }
            }
        }
        return memory;
    }
}
