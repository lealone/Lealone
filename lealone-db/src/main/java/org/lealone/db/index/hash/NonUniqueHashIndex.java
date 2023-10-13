/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index.hash;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lealone.common.util.Utils;
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
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 * @author zhh
 */
public class NonUniqueHashIndex extends HashIndex {

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ValueHashMap<ArrayList<Long>> rows;
    private long rowCount;

    public NonUniqueHashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        reset();
    }

    @Override
    protected void reset() {
        lock.writeLock().lock();
        try {
            rows = ValueHashMap.newInstance();
            rowCount = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        lock.writeLock().lock();
        try {
            Value key = getKey(row);
            ArrayList<Long> positions = rows.get(key);
            if (positions == null) {
                positions = Utils.newSmallArrayList();
                rows.put(key, positions);
            }
            positions.add(row.getKey());
            rowCount++;
        } finally {
            lock.writeLock().unlock();
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        lock.writeLock().lock();
        try {
            if (rowCount == 1) {
                // last row in table
                reset();
            } else {
                Value key = getKey(row);
                ArrayList<Long> positions = rows.get(key);
                if (positions.size() == 1) {
                    // last row with such key
                    rows.remove(key);
                } else {
                    positions.remove(row.getKey());
                }
                rowCount--;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        checkSearchKey(first, last);
        lock.readLock().lock();
        try {
            ArrayList<Long> list;
            ArrayList<Long> positions = rows.get(getKey(first));
            if (positions == null)
                list = new ArrayList<>(0);
            else
                // 这里必须copy一份，执行delete语句时会动态删除，这样会导致执行next()时漏掉一些记录
                list = new ArrayList<>(positions);
            return new NonUniqueHashCursor(session, table, list);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long getRowCount(ServerSession session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    /**
     * Cursor implementation for non-unique hash index
     */
    private static class NonUniqueHashCursor implements Cursor {

        private final ServerSession session;
        private final Table table;
        private final ArrayList<Long> positions;

        private int index = -1;

        public NonUniqueHashCursor(ServerSession session, Table table, ArrayList<Long> positions) {
            this.session = session;
            this.table = table;
            this.positions = positions;
        }

        @Override
        public Row get() {
            if (index < 0 || index >= positions.size()) {
                return null;
            }
            return table.getRow(session, positions.get(index));
        }

        @Override
        public boolean next() {
            return positions != null && ++index < positions.size();
        }
    }
}
