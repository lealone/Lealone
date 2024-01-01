/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index.standard;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.StandardTable;
import com.lealone.db.value.ValueLong;
import com.lealone.storage.CursorParameters;

/**
 * An index that delegates indexing to another index.
 * 
 * @author H2 Group
 * @author zhh
 */
public class StandardDelegateIndex extends StandardIndex {

    private final StandardPrimaryIndex mainIndex;

    public StandardDelegateIndex(StandardPrimaryIndex mainIndex, StandardTable table, int id,
            String name, IndexType indexType) {
        super(table, id, name, indexType,
                IndexColumn.wrap(new Column[] { table.getColumn(mainIndex.getMainIndexColumn()) }));
        this.mainIndex = mainIndex;
        if (id < 0) {
            throw DbException.getInternalError("" + name);
        }
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        return mainIndex.getRow(session, key);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        ValueLong min = mainIndex.getKey(first, StandardPrimaryIndex.MIN, StandardPrimaryIndex.MIN);
        // ifNull is MIN_VALUE as well, because the column is never NULL
        // so avoid returning all rows (returning one row is OK)
        ValueLong max = mainIndex.getKey(last, StandardPrimaryIndex.MAX, StandardPrimaryIndex.MIN);
        return mainIndex.find(session, min, max);
    }

    @Override
    public Cursor find(ServerSession session, CursorParameters<SearchRow> parameters) {
        return mainIndex.find(session, parameters);
    }

    @Override
    public SearchRow findFirstOrLast(ServerSession session, boolean first) {
        return mainIndex.findFirstOrLast(session, first);
    }

    @Override
    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndex.getMainIndexColumn()) {
            return 0;
        }
        return -1;
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCountApproximation(), sortOrder);
    }

    @Override
    public void remove(ServerSession session) {
        mainIndex.setMainIndexColumn(-1);
    }

    @Override
    public void truncate(ServerSession session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public long getRowCount(ServerSession session) {
        return mainIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return mainIndex.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return mainIndex.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return mainIndex.getMemorySpaceUsed();
    }

    @Override
    public boolean isInMemory() {
        return mainIndex.isInMemory();
    }
}
