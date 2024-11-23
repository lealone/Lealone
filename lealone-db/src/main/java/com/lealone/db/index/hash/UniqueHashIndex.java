/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.hash;

import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.row.Row;
import com.lealone.db.row.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;

//会有多个线程并发读写
public class UniqueHashIndex extends HashIndex<Long> {

    public UniqueHashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
    }

    @Override
    public void add(ServerSession session, Row row, AsyncResultHandler<Integer> handler) {
        Object old = rows.putIfAbsent(getIndexKey(row), row.getKey());
        if (old != null) {
            onException(handler, getDuplicateKeyException());
        } else {
            onComplete(handler);
        }
    }

    @Override
    public void remove(ServerSession session, Row row, Value[] oldColumns, boolean isLockedBySelf,
            AsyncResultHandler<Integer> handler) {
        rows.remove(getIndexKey(oldColumns));
        onComplete(handler);
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
