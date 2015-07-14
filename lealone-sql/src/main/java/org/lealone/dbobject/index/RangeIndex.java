/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.dbobject.index;

import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.RangeTable;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;
import org.lealone.value.ValueLong;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends IndexBase {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        initIndexBase(table, 0, "RANGE_INDEX", columns, IndexType.createNonUnique(true));
        this.rangeTable = table;
    }

    @Override
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

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 1;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new RangeCursor(pos, pos);
    }

    @Override
    public long getRowCount(Session session) {
        return rangeTable.getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return rangeTable.getRowCountApproximation();
    }

    /**
     * The cursor implementation for the range index.
     */
    private static class RangeCursor implements Cursor {

        private boolean beforeFirst;
        private long current;
        private Row currentRow;
        private final long min, max;

        RangeCursor(long min, long max) {
            this.min = min;
            this.max = max;
            beforeFirst = true;
        }

        @Override
        public Row get() {
            return currentRow;
        }

        @Override
        public SearchRow getSearchRow() {
            return currentRow;
        }

        @Override
        public boolean next() {
            if (beforeFirst) {
                beforeFirst = false;
                current = min;
            } else {
                current++;
            }
            currentRow = new Row(new Value[] { ValueLong.get(current) }, 1);
            return current <= max;
        }

        @Override
        public boolean previous() {
            throw DbException.throwInternalError();
        }
    }
}
