/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.util.Arrays;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockableBase;
import com.lealone.db.value.Value;

public class IndexKey extends LockableBase {

    Value[] columns;

    public IndexKey(Value[] columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return Arrays.toString(columns);
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
        IndexKey k = new IndexKey((Value[]) oldLockedValue);
        k.setLock(lock);
        return k;
    }
}
