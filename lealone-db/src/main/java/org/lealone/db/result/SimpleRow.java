/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.result;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;
import org.lealone.db.value.Value;

/**
 * Represents a simple row without state.
 */
public class SimpleRow extends RowBase {

    protected final Value[] data;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    @Override
    public int getColumnCount() {
        return data.length;
    }

    @Override
    public Value getValue(int index) {
        return data[index];
    }

    @Override
    public void setValue(int index, Value v) {
        data[index] = v;
    }

    @Override
    public int getMemory() {
        if (memory > 0) {
            return memory;
        }
        int m = Constants.MEMORY_ROW;
        if (data != null) {
            int len = data.length;
            m += Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (int i = 0; i < len; i++) {
                Value v = data[i];
                if (v != null) {
                    m += v.getMemory();
                }
            }
        }
        memory = m;
        return m;
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        buff.append(" */ ");
        if (data != null) {
            for (Value v : data) {
                buff.appendExceptFirst(", ");
                buff.append(v == null ? "null" : v.getTraceSQL());
            }
        }
        return buff.append(')').toString();
    }
}
