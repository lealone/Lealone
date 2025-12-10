/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.lock.LockableBase;
import com.lealone.db.row.Row;
import com.lealone.db.value.Value;

public abstract class IndexKey extends LockableBase {

    private long key;

    public IndexKey(long key) {
        this.key = key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    public abstract Value[] getColumns();

    @Override
    public String toString() {
        return Row.toString(key, getColumns());
    }

    public static IndexKey create(long key, Value[] columns) {
        if (columns != null && columns.length == 1) {
            return new SingleIndexKey(key, columns[0]);
        } else {
            return new CompoundIndexKey(key, columns);
        }
    }

    public static class SingleIndexKey extends IndexKey {

        private Value column;

        public SingleIndexKey(long key, Value column) {
            super(key);
            this.column = column;
        }

        @Override
        public Value[] getColumns() {
            return new Value[] { column };
        }

        @Override
        public void setLockedValue(Object value) {
            if (value instanceof SingleIndexKey)
                column = ((SingleIndexKey) value).column;
            else
                column = (Value) value;
        }

        @Override
        public Object getLockedValue() {
            return column;
        }

        @Override
        public Object copy(Object oldLockedValue, Lock lock) {
            SingleIndexKey k = new SingleIndexKey(getKey(), (Value) oldLockedValue);
            k.setLock(lock);
            return k;
        }

        @Override
        public Lockable copySelf(Object oldLockedValue) {
            return new SingleIndexKey(getKey(), (Value) oldLockedValue);
        }
    }

    public static class CompoundIndexKey extends IndexKey {

        private Value[] columns;

        public CompoundIndexKey(long key, Value[] columns) {
            super(key);
            this.columns = columns;
        }

        @Override
        public Value[] getColumns() {
            return columns;
        }

        @Override
        public void setLockedValue(Object value) {
            if (value instanceof CompoundIndexKey)
                columns = ((CompoundIndexKey) value).columns;
            else
                columns = (Value[]) value;
        }

        @Override
        public Object getLockedValue() {
            return columns;
        }

        @Override
        public Object copy(Object oldLockedValue, Lock lock) {
            CompoundIndexKey k = new CompoundIndexKey(getKey(), (Value[]) oldLockedValue);
            k.setLock(lock);
            return k;
        }

        @Override
        public Lockable copySelf(Object oldLockedValue) {
            return new CompoundIndexKey(getKey(), (Value[]) oldLockedValue);
        }
    }
}
