/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index.hash;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.StandardTable;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 * @author zhh
 */
public class NonUniqueHashIndex extends HashIndex {

    private ValueHashMap<ArrayList<Long>> rows;
    private long rowCount;

    public NonUniqueHashIndex(StandardTable table, int id, String indexName, IndexColumn[] columns,
            IndexType indexType) {
        super(table, id, indexName, indexType, columns);
        reset();
    }

    @Override
    protected void reset() {
        rows = ValueHashMap.newInstance();
        rowCount = 0;
    }

    @Override
    public void add(ServerSession session, Row row) {
        Value key = row.getValue(indexColumn);
        ArrayList<Long> positions = rows.get(key);
        if (positions == null) {
            positions = Utils.newSmallArrayList();
            rows.put(key, positions);
        }
        positions.add(row.getKey());
        rowCount++;
    }

    @Override
    public void remove(ServerSession session, Row row) {
        if (rowCount == 1) {
            // last row in table
            reset();
        } else {
            Value key = row.getValue(indexColumn);
            ArrayList<Long> positions = rows.get(key);
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.remove(row.getKey());
            }
            rowCount--;
        }
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            throw DbException.throwInternalError();
        }
        if (first != last) {
            if (compareKeys(first, last) != 0) {
                throw DbException.throwInternalError();
            }
        }
        ArrayList<Long> positions = rows.get(first.getValue(indexColumn));
        return new NonUniqueHashCursor(session, table, positions);
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
     *
     * @author Sergi Vladykin
     */
    private static class NonUniqueHashCursor implements Cursor {

        private final ServerSession session;
        private final ArrayList<Long> positions;
        private final StandardTable table;

        private int index = -1;

        public NonUniqueHashCursor(ServerSession session, StandardTable table, ArrayList<Long> positions) {
            this.session = session;
            this.table = table;
            // 这里必须copy一份，执行delete语句时会动态删除，这样会导致执行next()时漏掉一些记录
            this.positions = new ArrayList<>(positions);
        }

        @Override
        public Row get() {
            if (index < 0 || index >= positions.size()) {
                return null;
            }
            return table.getRow(session, positions.get(index));
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            return positions != null && ++index < positions.size();
        }
    }
}
