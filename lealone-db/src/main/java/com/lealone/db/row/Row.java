/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.row;

import com.lealone.common.util.MathUtils;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.lock.LockableBase;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDataType.PrimaryKey;
import com.lealone.db.value.ValueLong;

/**
 * Represents a row in a table.
 */
public class Row extends LockableBase implements SearchRow, PrimaryKey, Comparable<Row> {

    private long key;
    private Value[] columns;

    public Row(Value[] columns) {
        this.columns = columns;
    }

    public Row(long key, Value[] columns) {
        this.key = key;
        this.columns = columns;
    }

    @Override
    public ValueLong getPrimaryKey() {
        return ValueLong.get(key);
    }

    @Override
    public Value[] getColumns() {
        return columns;
    }

    public void setColumns(Value[] columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() {
        return columns.length;
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
    public Value getValue(int i) {
        return columns[i];
    }

    @Override
    public void setValue(int i, Value v) {
        if (i < 0)
            key = v.getLong();
        else
            columns[i] = v;
    }

    ////////////// 以下是Lockable接口的实现 //////////////

    @Override
    public void setKey(Object key) {
        if (key instanceof ValueLong) {
            setKey(((ValueLong) key).getLong());
        }
    }

    @Override
    public void setLockedValue(Object value) {
        if (value instanceof Row)
            columns = ((Row) value).columns;
        else
            columns = (Value[]) value;
    }

    @Override
    public Object getLockedValue() {
        return columns;
    }

    @Override
    public Object copy(Object oldLockedValue, Lock lock) {
        Row row = new Row(getKey(), (Value[]) oldLockedValue);
        row.setLock(lock);
        return row;
    }

    @Override
    public Lockable copySelf(Object oldLockedValue) {
        return new Row(getKey(), (Value[]) oldLockedValue);
    }

    @Override
    public int compareTo(Row o) {
        return MathUtils.compareLong(this.key, o.key);
    }

    @Override
    public String toString() {
        return toString(key, columns);
    }

    public static String toString(long key, Value[] columns) {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(key);
        buff.append(" */ ");
        if (columns != null) {
            for (Value v : columns) {
                buff.appendExceptFirst(", ");
                buff.append(v == null ? "null" : v.getTraceSQL());
            }
        }
        return buff.append(')').toString();
    }
}
