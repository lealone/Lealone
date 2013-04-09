/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.dbobject.index;

import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;

/**
 * A cursor with at most one row.
 */
public class SingleRowCursor implements Cursor {
    private Row row;
    private boolean end;

    /**
     * Create a new cursor.
     *
     * @param row - the single row (if null then cursor is empty)
     */
    public SingleRowCursor(Row row) {
        this.row = row;
    }

    public Row get() {
        return row;
    }

    public SearchRow getSearchRow() {
        return row;
    }

    public boolean next() {
        if (row == null || end) {
            row = null;
            return false;
        }
        end = true;
        return true;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
