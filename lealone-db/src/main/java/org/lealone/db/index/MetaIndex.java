/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.ArrayList;

import org.lealone.common.message.DbException;
import org.lealone.db.Session;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.MetaTable;
import org.lealone.db.table.TableFilter;

/**
 * The index implementation for meta data tables.
 */
public class MetaIndex extends IndexBase {

    private final MetaTable meta;
    private final boolean scan;

    public MetaIndex(MetaTable meta, IndexColumn[] columns, boolean scan) {
        initIndexBase(meta, 0, null, columns, IndexType.createNonUnique(true));
        this.meta = meta;
        this.scan = scan;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ArrayList<Row> rows = meta.generateRows(session, first, last);
        return new MetaCursor(rows);
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        if (scan) {
            return 10 * MetaTable.ROW_COUNT_APPROXIMATION;
        }
        return getCostRangeIndex(masks, MetaTable.ROW_COUNT_APPROXIMATION, filter, sortOrder);
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
    public String getCreateSQL() {
        return null;
    }

    @Override
    public long getRowCount(Session session) {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getRowCountApproximation() {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getDiskSpaceUsed() {
        return meta.getDiskSpaceUsed();
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

        private Row current;
        private final ArrayList<Row> rows;
        private int index;

        MetaCursor(ArrayList<Row> rows) {
            this.rows = rows;
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public SearchRow getSearchRow() {
            return current;
        }

        @Override
        public boolean next() {
            current = index >= rows.size() ? null : rows.get(index++);
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.throwInternalError();
        }

    }
}
