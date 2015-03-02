/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.wiredtiger.dbobject.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.api.ErrorCode;
import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.engine.Constants;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;
import org.lealone.value.ValueLong;
import org.lealone.value.ValueNull;
import org.lealone.wiredtiger.dbobject.table.WiredTigerTable;

public class WiredTigerPrimaryIndex extends BaseIndex {

    /**
     * The minimum long value.
     */
    static final ValueLong MIN = ValueLong.get(Long.MIN_VALUE);

    /**
     * The maximum long value.
     */
    static final ValueLong MAX = ValueLong.get(Long.MAX_VALUE);

    /**
     * The zero long value.
     */
    static final ValueLong ZERO = ValueLong.get(0);

    private final ConcurrentNavigableMap<Value, Row> rows = new ConcurrentSkipListMap<Value, Row>();
    private final WiredTigerTable table;
    private long lastKey;
    private int mainIndexColumn = -1;

    public WiredTigerPrimaryIndex(Database db, WiredTigerTable table, int id, IndexColumn[] columns, IndexType indexType) {
        this.table = table;
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    private Value getKey(Row row) {
        Value key = ValueLong.get(row.getKey());
        key.compareMode = table.getCompareMode();
        return key;
    }

    private Value getKey(Value v) {
        v.compareMode = table.getCompareMode();
        return v;
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                row.setKey(++lastKey);
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        if (table.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                Value v2 = v.link(database, getId());
                if (v2.isLinked()) {
                    session.unlinkAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }

        Value key = getKey(row);
        Row old = rows.get(key);
        if (row.isUpdate()) {
            old.merge(row);
        } else {
            if (old != null) {
                String sql = "PRIMARY KEY ON " + table.getSQL();
                if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                    sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
                }
                DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
                e.setSource(this);
                throw e;
            }
            try {
                rows.put(key, row);
            } catch (IllegalStateException e) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
            }
            lastKey = Math.max(lastKey, row.getKey());
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (table.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        Value key = getKey(row);
        Row old = rows.get(key);
        try {
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, getSQL() + ": " + row.getKey());
            }
            if (row.isUpdate())
                row.cleanUpdateFlag(); //由update语句触发，什么都不做
            else
                //不删除行，只设个删除标记
                old.setDeleted(true); //rows.remove(key);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        Value min, max;
        if (first == null) {
            min = MIN;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            Value v = first.getValue(mainIndexColumn);
            if (v == null) {
                min = ValueLong.get(first.getKey());
            } else {
                min = v;
            }
        }
        if (last == null) {
            max = MAX;
        } else if (mainIndexColumn < 0) {
            max = ValueLong.get(last.getKey());
        } else {
            Value v = last.getValue(mainIndexColumn);
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        return find(session, min, max);
    }

    @Override
    public WiredTigerTable getTable() {
        return table;
    }

    @Override
    public Row getRow(Session session, long key) {
        return rows.get(ValueLong.get(key));
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        try {
            long cost = 10 * (table.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
            return cost;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }

    @Override
    public void remove(Session session) {
        //TODO
    }

    @Override
    public void truncate(Session session) {
        if (table.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        Row row = (first ? rows.firstEntry().getValue() : rows.lastEntry().getValue());
        if (row == null) {
            return new WiredTigerPrimaryIndexCursor(Collections.<Entry<Value, Row>> emptyList().iterator(), null, session
                    .getTransaction().getTransactionId());
        }
        ValueLong key = ValueLong.get(row.getKey());
        HashMap<Value, Row> e = new HashMap<>(1);
        e.put(getKey(key), row);
        WiredTigerPrimaryIndexCursor c = new WiredTigerPrimaryIndexCursor(e.entrySet().iterator(), key, session.getTransaction()
                .getTransactionId());
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return rows.size();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return rows.size();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCountApproximation() {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    ValueLong getKey(SearchRow row, ValueLong ifEmpty, ValueLong ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            throw DbException.throwInternalError(row.toString());
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return (ValueLong) v.convertTo(Value.LONG);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, Value first, Value last) {
        return new WiredTigerPrimaryIndexCursor(rows.tailMap(getKey(first)).entrySet().iterator(), last, session.getTransaction()
                .getTransactionId());
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * A cursor.
     */
    private static class WiredTigerPrimaryIndexCursor implements Cursor {

        private final Iterator<Entry<Value, Row>> it;
        private final Value last;
        private Entry<Value, Row> current;
        private Row row;

        private final long transactionId;

        public WiredTigerPrimaryIndexCursor(Iterator<Entry<Value, Row>> it, Value last, long transactionId) {
            this.it = it;
            this.last = last;
            this.transactionId = transactionId;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = current.getValue();
                }
            }
            if (row != null)
                row.setTransactionId(transactionId);
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            if (current != null && current.getKey().getLong() > last.getLong()) {
                current = null;
            }
            if (current != null && current.getValue().isDeleted()) //过滤掉已删除的行
                return next();

            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

}
