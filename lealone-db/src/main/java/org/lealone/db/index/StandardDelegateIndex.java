/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.StandardTable;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.ValueLong;

/**
 * An index that delegates indexing to another index.
 */
public class StandardDelegateIndex extends IndexBase implements StandardIndex {

    private final StandardPrimaryIndex mainIndex;

    public StandardDelegateIndex(StandardTable table, int id, String name, StandardPrimaryIndex mainIndex,
            IndexType indexType) {
        super(table, id, name, indexType, IndexColumn.wrap(new Column[] { table.getColumn(mainIndex
                .getMainIndexColumn()) }));
        this.mainIndex = mainIndex;
        if (id < 0) {
            throw DbException.throwInternalError("" + name);
        }
    }

    @Override
    public void addRowsToBuffer(ServerSession session, List<Row> rows, String bufferName) {
        throw DbException.throwInternalError();
    }

    @Override
    public void addBufferedRows(ServerSession session, List<String> bufferNames) {
        throw DbException.throwInternalError();
    }

    @Override
    public void add(ServerSession session, Row row) {
        // nothing to do
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
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
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
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
    public double getCost(ServerSession session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCountApproximation(), filter, sortOrder);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(ServerSession session, Row row) {
        // nothing to do
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
        return 0;
    }

    @Override
    public boolean isInMemory() {
        return mainIndex.isInMemory();
    }

}
