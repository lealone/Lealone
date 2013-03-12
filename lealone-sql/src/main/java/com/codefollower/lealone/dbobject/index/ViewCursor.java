/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.dbobject.index;

import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueNull;

/**
 * The cursor implementation of a view index.
 */
public class ViewCursor implements Cursor {

    private final Table table;
    private final Index index;
    private final ResultInterface result;
    private final SearchRow first, last;
    private Row current;

    ViewCursor(Index index, ResultInterface result, SearchRow first, SearchRow last) {
        this.table = index.getTable();
        this.index = index;
        this.result = result;
        this.first = first;
        this.last = last;
    }

    public Row get() {
        return current;
    }

    public SearchRow getSearchRow() {
        return current;
    }

    public boolean next() {
        while (true) {
            boolean res = result.next();
            if (!res) {
                result.close();
                current = null;
                return false;
            }
            current = table.getTemplateRow();
            Value[] values = result.currentRow();
            for (int i = 0, len = current.getColumnCount(); i < len; i++) {
                Value v = i < values.length ? values[i] : ValueNull.INSTANCE;
                current.setValue(i, v);
            }
            int comp;
            if (first != null) {
                comp = index.compareRows(current, first);
                if (comp < 0) {
                    continue;
                }
            }
            if (last != null) {
                comp = index.compareRows(current, last);
                if (comp > 0) {
                    continue;
                }
            }
            return true;
        }
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
