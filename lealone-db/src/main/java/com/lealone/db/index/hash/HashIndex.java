/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.hash;

import java.util.concurrent.ConcurrentHashMap;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexBase;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexConditionType;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.SortOrder;
import com.lealone.db.row.Row;
import com.lealone.db.row.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;

public abstract class HashIndex<T> extends IndexBase {

    protected final ConcurrentHashMap<Value, T> rows = new ConcurrentHashMap<>();
    protected final int indexColumnId;

    protected HashIndex(Table table, int id, String indexName, IndexType indexType,
            IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        this.indexColumnId = columns[0].column.getColumnId();
    }

    protected Value getIndexKey(SearchRow row) {
        return row.getValue(indexColumnId);
    }

    protected Value getIndexKey(Value[] columns) {
        return columns[indexColumnId];
    }

    protected void checkSearchKey(SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            throw DbException.getInternalError();
        }
        if (first != last) {
            if (!getIndexKey(first).equals(getIndexKey(last))) {
                throw DbException.getInternalError();
            }
        }
    }

    @Override
    public void truncate(ServerSession session) {
        rows.clear();
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }

    @Override
    public void remove(ServerSession session) {
        // nothing to do
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        if (masks == null) {
            return Long.MAX_VALUE;
        }
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexConditionType.EQUALITY) != IndexConditionType.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public SearchRow findFirstOrLast(ServerSession session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    @Override
    public boolean canScan() {
        return false;
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    protected static abstract class HashCursor implements Cursor {

        protected Row row;

        @Override
        public Row get() {
            return row;
        }
    }
}
