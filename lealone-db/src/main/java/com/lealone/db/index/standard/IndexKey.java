/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockableBase;
import com.lealone.db.row.Row;
import com.lealone.db.value.Value;

public class IndexKey extends LockableBase {

    private long key;
    private Value[] columns;

    public IndexKey(long key, Value[] columns) {
        this.key = key;
        this.columns = columns;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    public Value[] getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return Row.toString(key, columns);
    }

    @Override
    public void setLockedValue(Object value) {
        if (value instanceof IndexKey)
            columns = ((IndexKey) value).columns;
        else
            columns = (Value[]) value;
    }

    @Override
    public Object getLockedValue() {
        return columns;
    }

    @Override
    public Object copy(Object oldLockedValue, Lock lock) {
        IndexKey k = new IndexKey(key, (Value[]) oldLockedValue);
        k.setLock(lock);
        return k;
    }
}
