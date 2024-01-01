/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index.hash;

import java.util.concurrent.ConcurrentHashMap;

import com.lealone.db.async.Future;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.transaction.Transaction;

/**
 * An unique index based on an in-memory hash map.
 * 
 * @author H2 Group
 * @author zhh
 */
public class UniqueHashIndex extends HashIndex {

    private ConcurrentHashMap<Value, Long> rows;

    public UniqueHashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        reset();
    }

    @Override
    protected void reset() {
        rows = new ConcurrentHashMap<>();
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        Object old = rows.putIfAbsent(getKey(row), row.getKey());
        if (old != null) {
            throw getDuplicateKeyException();
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        rows.remove(getKey(row));
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        checkSearchKey(first, last);
        Row result;
        Long pos = rows.get(getKey(first));
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
