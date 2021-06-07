/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index.hash;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.Future;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.transaction.Transaction;

/**
 * An unique index based on an in-memory hash map.
 * 
 * @author H2 Group
 * @author zhh
 */
public class UniqueHashIndex extends HashIndex {

    private ValueHashMap<Long> rows;

    public UniqueHashIndex(Table table, int id, String indexName, IndexType indexType, IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        reset();
    }

    @Override
    protected void reset() {
        rows = ValueHashMap.newInstance();
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        Value key = row.getValue(indexColumn);
        Object old = rows.get(key);
        if (old != null) {
            throw getDuplicateKeyException();
        }
        rows.put(key, row.getKey());
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row) {
        rows.remove(row.getValue(indexColumn));
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw DbException.getInternalError();
        }
        Row result;
        Long pos = rows.get(first.getValue(indexColumn));
        if (pos == null) {
            result = null;
        } else {
            result = table.getRow(session, pos.intValue());
        }
        return new SingleRowCursor(result);
    }

    @Override
    public long getRowCount(ServerSession session) {
        return getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return rows.size();
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
    }
}
