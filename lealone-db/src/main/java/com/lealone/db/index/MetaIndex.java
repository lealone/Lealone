/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index;

import java.util.ArrayList;

import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.MetaTable;

/**
 * The index implementation for meta data tables.
 */
public class MetaIndex extends IndexBase {

    private final MetaTable meta;
    private final boolean scan;

    public MetaIndex(MetaTable meta, IndexColumn[] columns, boolean scan) {
        super(meta, 0, null, IndexType.createNonUnique(), columns);
        this.meta = meta;
        this.scan = scan;
    }

    @Override
    public int getColumnIndex(Column col) {
        if (scan) {
            // the scan index cannot use any columns
            return -1;
        }
        return super.getColumnIndex(col);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        ArrayList<Row> rows = meta.generateRows(session, first, last);
        return new MetaCursor(rows);
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        if (scan) {
            return 10 * MetaTable.ROW_COUNT_APPROXIMATION;
        }
        return getCostRangeIndex(masks, MetaTable.ROW_COUNT_APPROXIMATION, sortOrder);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return "meta";
    }

    /**
     * An index for a meta data table.
     * This index can only scan through all rows, search is not supported.
     */
    private static class MetaCursor implements Cursor {

        private final ArrayList<Row> rows;
        private Row current;
        private int index;

        MetaCursor(ArrayList<Row> rows) {
            this.rows = rows;
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public boolean next() {
            current = index >= rows.size() ? null : rows.get(index++);
            return current != null;
        }
    }
}
