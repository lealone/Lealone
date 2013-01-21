/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject.index;

import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.SearchRow;
import com.codefollower.yourbase.value.Value;

/**
 * A cursor for a function that returns a result.
 */
public class FunctionCursor implements Cursor {

    private final ResultInterface result;
    private Value[] values;
    private Row row;

    FunctionCursor(ResultInterface result) {
        this.result = result;
    }

    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = new Row(values, 1);
        }
        return row;
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public boolean next() {
        row = null;
        if (result != null && result.next()) {
            values = result.currentRow();
        } else {
            values = null;
        }
        return values != null;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
