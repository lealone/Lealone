/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.hash;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.LinkedTransferQueue;

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

//单个线程写多个线程读
public class NonUniqueHashIndex extends HashIndex<LinkedTransferQueue<Long>> {

    public NonUniqueHashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        Value indexKey = getIndexKey(row);
        return update(indexKey, row, true);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, Value[] oldColumns,
            boolean isLockedBySelf) {
        Value indexKey = getIndexKey(oldColumns);
        return update(indexKey, row, false);
    }

    private Future<Integer> update(Value indexKey, Row row, boolean add) {
        Long rowKey = Long.valueOf(row.getKey());
        LinkedTransferQueue<Long> rowKeys = rows.get(indexKey);
        if (rowKeys == null) {
            if (add) {
                rowKeys = new LinkedTransferQueue<>();
                rows.put(indexKey, rowKeys);
            } else {
                return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
            }
        }
        if (add) {
            rowKeys.add(rowKey);
        } else {
            rowKeys.remove(rowKey);
            if (rowKeys.isEmpty())
                rows.remove(indexKey);
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        checkSearchKey(first, last);
        LinkedTransferQueue<Long> rowKeys = rows.get(getIndexKey(first));
        if (rowKeys == null)
            return new NonUniqueHashCursor(session, table, Collections.emptyIterator());
        else
            return new NonUniqueHashCursor(session, table, rowKeys.iterator());
    }

    private static class NonUniqueHashCursor extends HashCursor {

        private final ServerSession session;
        private final Table table;
        private final Iterator<Long> rowKeyIterator;

        public NonUniqueHashCursor(ServerSession session, Table table, Iterator<Long> rowKeyIterator) {
            this.session = session;
            this.table = table;
            this.rowKeyIterator = rowKeyIterator;
        }

        @Override
        public boolean next() {
            while (true) {
                if (rowKeyIterator.hasNext()) {
                    long rowKey = rowKeyIterator.next();
                    row = table.getRow(session, rowKey);
                    if (row != null)
                        return true;
                } else {
                    row = null;
                    return false;
                }
            }
        }
    }
}
