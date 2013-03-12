/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.dbobject.index;

import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueLong;

/**
 * The cursor implementation for the range index.
 */
class RangeCursor implements Cursor {

    private boolean beforeFirst;
    private long current;
    private Row currentRow;
    private final long min, max;

    RangeCursor(long min, long max) {
        this.min = min;
        this.max = max;
        beforeFirst = true;
    }

    public Row get() {
        return currentRow;
    }

    public SearchRow getSearchRow() {
        return currentRow;
    }

    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            current = min;
        } else {
            current++;
        }
        currentRow = new Row(new Value[]{ValueLong.get(current)}, 1);
        return current <= max;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
