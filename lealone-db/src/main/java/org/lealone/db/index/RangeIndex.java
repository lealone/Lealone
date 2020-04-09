/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.RangeTable;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends IndexBase {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        super(table, 0, "RANGE_INDEX", IndexType.createNonUnique(), columns);
        this.rangeTable = table;
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
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
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
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
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new RangeCursor(pos, pos);
    }

    @Override
    public long getRowCount(ServerSession session) {
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
    }
}
