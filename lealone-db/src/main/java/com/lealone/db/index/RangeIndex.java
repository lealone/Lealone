/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.RangeTable;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;

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
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long step = rangeTable.getStep(session);
        if (first != null) {
            try {
                long v = first.getValue(0).getLong();
                if (step > 0) {
                    if (v > min) {
                        min += (v - min + step - 1) / step * step;
                    }
                } else if (v > max) {
                    max = v;
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        if (last != null) {
            try {
                long v = last.getValue(0).getLong();
                if (step > 0) {
                    if (v < max) {
                        max = v;
                    }
                } else if (v < min) {
                    min -= (min - v - step - 1) / step * step;
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        return new RangeCursor(min, max, step);
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public SearchRow findFirstOrLast(ServerSession session, boolean first) {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new Row(new Value[] { ValueLong.get(pos) });
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        return 1;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    /**
     * The cursor implementation for the range index.
     */
    private static class RangeCursor implements Cursor {

        private final long start, end, step;
        private boolean beforeFirst;
        private long current;
        private Row currentRow;

        RangeCursor(long start, long end, long step) {
            this.start = start;
            this.end = end;
            this.step = step;
            beforeFirst = true;
        }

        @Override
        public Row get() {
            return currentRow;
        }

        @Override
        public boolean next() {
            if (beforeFirst) {
                beforeFirst = false;
                current = start;
            } else {
                current += step;
            }
            currentRow = new Row(new Value[] { ValueLong.get(current) });
            return step > 0 ? current <= end : current >= end;
        }
    }
}
