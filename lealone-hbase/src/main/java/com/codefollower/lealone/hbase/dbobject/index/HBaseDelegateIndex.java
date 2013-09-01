/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.hbase.dbobject.index;

import com.codefollower.lealone.constant.Constants;
import com.codefollower.lealone.dbobject.index.BaseIndex;
import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.result.SortOrder;

/**
 * An index that delegates indexing to another index.
 */
public class HBaseDelegateIndex extends BaseIndex {

    private final HBasePrimaryIndex mainIndex;

    public HBaseDelegateIndex(HBaseTable table, int id, String name, IndexColumn[] cols, HBasePrimaryIndex mainIndex,
            IndexType indexType) {
        if (id < 0)
            throw DbException.throwInternalError(name);
        this.initBaseIndex(table, id, name, cols, indexType);
        this.mainIndex = mainIndex;
    }

    @Override
    public void add(Session session, Row row) {
        // nothing to do
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        setRowKey(first, last);
        return mainIndex.find(session, first, last);
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        setRowKey(first, last);
        return mainIndex.find(filter, first, last);
    }

    private void setRowKey(SearchRow first, SearchRow last) {
        int columnId = getColumns()[0].getColumnId();
        if (first != null)
            first.setRowKey(first.getValue(columnId));
        if (last != null)
            last.setRowKey(last.getValue(columnId));
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return mainIndex.findFirstOrLast(session, first);
    }

    @Override
    public int getColumnIndex(Column col) {
        if (col.getColumnId() == columns[0].getColumnId()) {
            return 0;
        }
        return -1;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        int mask = masks[columns[0].getColumnId()];
        if (mask != 0)
            return 3;
        else
            return 10 * Constants.COST_ROW_OFFSET;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(Session session, Row row) {
        // nothing to do
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public long getRowCount(Session session) {
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

}
