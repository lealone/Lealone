/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.row;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockableBase;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDataType.PrimaryKey;
import com.lealone.db.value.ValueLong;

/**
 * Represents a row in a table.
 */
public class Row extends LockableBase implements SearchRow, PrimaryKey {

    private long key;
    private Value[] columns;
    private int version; // 表的元数据版本号

    public Row(Value[] columns) {
        this.columns = columns;
    }

    public Row(long key, Value[] columns) {
        this(columns);
        this.key = key;
    }

    public Row(int version, Value[] columns) {
        this(columns);
        this.version = version;
    }

    @Override
    public ValueLong getPrimaryKey() {
        return ValueLong.get(key);
    }

    @Override
    public Value[] getColumns() {
        return columns;
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
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public Value getValue(int i) {
        return columns[i];
    }

    @Override
    public void setValue(int i, Value v) {
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
        Row row = new Row(getVersion(), (Value[]) oldLockedValue);
        row.setKey(getKey());
        row.setLock(lock);
        return row;
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
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
