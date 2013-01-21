/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject.index;

import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.RegularTable;
import com.codefollower.yourbase.engine.RegularDatabase;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.SearchRow;
import com.codefollower.yourbase.store.PageStore;

/**
 * An index that delegates indexing to the page data index.
 */
public class PageDelegateIndex extends PageIndex {

    private final PageDataIndex mainIndex;

    public PageDelegateIndex(RegularTable table, int id, String name,
            IndexType indexType, PageDataIndex mainIndex, boolean create, Session session) {
        IndexColumn[] cols = IndexColumn.wrap(new Column[] { table.getColumn(mainIndex.getMainIndexColumn())});
        this.initBaseIndex(table, id, name, cols, indexType);
        RegularDatabase database = (RegularDatabase) this.database;
        this.mainIndex = mainIndex;
        if (!database.isPersistent() || id < 0) {
            throw DbException.throwInternalError("" + name);
        }
        PageStore store = database.getPageStore();
        store.addIndex(this);
        if (create) {
            store.addMeta(this, session);
        }
    }

    public void add(Session session, Row row) {
        // nothing to do
    }

    public boolean canFindNext() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public void close(Session session) {
        // nothing to do
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min = mainIndex.getKey(first, Long.MIN_VALUE, Long.MIN_VALUE);
        // ifNull is MIN_VALUE as well, because the column is never NULL
        // so avoid returning all rows (returning one row is OK)
        long max = mainIndex.getKey(last, Long.MAX_VALUE, Long.MIN_VALUE);
        return mainIndex.find(session, min, max, false);
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        Cursor cursor;
        if (first) {
            cursor = mainIndex.find(session, Long.MIN_VALUE, Long.MAX_VALUE, false);
        } else  {
            long x = mainIndex.getLastKey();
            cursor = mainIndex.find(session, x, x, false);
        }
        cursor.next();
        return cursor;
    }

    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        throw DbException.throwInternalError();
    }

    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndex.getMainIndexColumn()) {
            return 0;
        }
        return -1;
    }

    public double getCost(Session session, int[] masks) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCount(session));
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) {
        // nothing to do
    }

    public void remove(Session session) {
        RegularDatabase database = (RegularDatabase) session.getDatabase();
        mainIndex.setMainIndexColumn(-1);
        database.getPageStore().removeMeta(this, session);
    }

    public void truncate(Session session) {
        // nothing to do
    }

    public void checkRename() {
        // ok
    }

    public long getRowCount(Session session) {
        return mainIndex.getRowCount(session);
    }

    public long getRowCountApproximation() {
        return mainIndex.getRowCountApproximation();
    }

    public long getDiskSpaceUsed() {
        return mainIndex.getDiskSpaceUsed();
    }

    public void writeRowCount() {
        // ignore
    }

}
