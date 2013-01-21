/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject.index;

import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.RangeTable;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.SearchRow;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends BaseIndex {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        initBaseIndex(table, 0, "RANGE_INDEX", columns, IndexType.createNonUnique(true));
        this.rangeTable = table;
    }

    public void close(Session session) {
        // nothing to do
    }

    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min = rangeTable.getMin(session), start = min;
        long max = rangeTable.getMax(session), end = max;
        try {
            start = Math.max(min, first == null ? min : first.getValue(0).getLong());
        } catch (Exception e) {
            // error when converting the value - ignore
        }
        try {
            end = Math.min(max, last == null ? max : last.getValue(0).getLong());
        } catch (Exception e) {
            // error when converting the value - ignore
        }
        return new RangeCursor(start, end);
    }

    public double getCost(Session session, int[] masks) {
        return 1;
    }

    public String getCreateSQL() {
        return null;
    }

    public void remove(Session session) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    public void truncate(Session session) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    public boolean needRebuild() {
        return false;
    }

    public void checkRename() {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new RangeCursor(pos, pos);
    }

    public long getRowCount(Session session) {
        return rangeTable.getRowCountApproximation();
    }

    public long getRowCountApproximation() {
        return rangeTable.getRowCountApproximation();
    }

    public long getDiskSpaceUsed() {
        return 0;
    }
}
