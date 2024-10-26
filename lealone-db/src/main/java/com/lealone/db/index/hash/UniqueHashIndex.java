/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.hash;

import com.lealone.db.async.Future;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.row.Row;
import com.lealone.db.row.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.transaction.Transaction;

//会有多个线程并发读写
public class UniqueHashIndex extends HashIndex<Long> {

    public UniqueHashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        Object old = rows.putIfAbsent(getIndexKey(row), row.getKey());
        if (old != null) {
            throw getDuplicateKeyException();
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, Value[] oldColumns,
            boolean isLockedBySelf) {
        rows.remove(getIndexKey(oldColumns));
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        checkSearchKey(first, last);
        Row row;
        Long rowKey = rows.get(getIndexKey(first));
        if (rowKey == null) {
            row = null;
        } else {
            row = table.getRow(session, rowKey.longValue());
        }
        return new UniqueHashCursor(row);
    }

    private static class UniqueHashCursor extends HashCursor {

        private boolean end;

        public UniqueHashCursor(Row row) {
            this.row = row; // 可能为null
        }

        @Override
        public boolean next() {
            if (row == null || end) {
                row = null;
                return false;
            } else {
                end = true;
                return true;
            }
        }
    }
}
