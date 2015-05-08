/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.mvdb.dbobject.index;

import org.lealone.dbobject.index.IndexBase;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexCondition;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.util.ValueHashMap;
import org.lealone.value.Value;

/**
 * An unique index based on an in-memory hash map.
 */
public class HashIndex extends IndexBase {

    /**
     * The index of the indexed column.
     */
    protected final int indexColumn;

    private final TableBase tableData;
    private ValueHashMap<Long> rows;

    public HashIndex(TableBase table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initIndexBase(table, id, indexName, columns, indexType);
        this.indexColumn = columns[0].column.getColumnId();
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance();
    }

    @Override
    public void truncate(Session session) {
        reset();
    }

    @Override
    public void add(Session session, Row row) {
        Value key = row.getValue(indexColumn);
        Object old = rows.get(key);
        if (old != null) {
            // TODO index duplicate key for hash indexes: is this allowed?
            throw getDuplicateKeyException();
        }
        rows.put(key, row.getKey());
    }

    @Override
    public void remove(Session session, Row row) {
        rows.remove(row.getValue(indexColumn));
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw DbException.throwInternalError();
        }
        Row result;
        Long pos = rows.get(first.getValue(indexColumn));
        if (pos == null) {
            result = null;
        } else {
            result = tableData.getRow(session, pos.intValue());
        }
        return new SingleRowCursor(result);
    }

    @Override
    public long getRowCount(Session session) {
        return getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return rows.size();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    @Override
    public boolean canScan() {
        return false;
    }

    /**
     * A cursor with at most one row.
     */
    private static class SingleRowCursor implements Cursor {
        private Row row;
        private boolean end;

        /**
         * Create a new cursor.
         *
         * @param row - the single row (if null then cursor is empty)
         */
        public SingleRowCursor(Row row) {
            this.row = row;
        }

        @Override
        public Row get() {
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return row;
        }

        @Override
        public boolean next() {
            if (row == null || end) {
                row = null;
                return false;
            }
            end = true;
            return true;
        }

        @Override
        public boolean previous() {
            throw DbException.throwInternalError();
        }

    }

}
