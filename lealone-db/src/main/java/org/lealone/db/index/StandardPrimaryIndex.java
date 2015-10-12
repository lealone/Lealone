/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.value.Value;
import org.lealone.common.value.ValueArray;
import org.lealone.common.value.ValueLong;
import org.lealone.common.value.ValueNull;
import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.StandardTable;
import org.lealone.db.table.TableFilter;
import org.lealone.storage.Storage;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class StandardPrimaryIndex extends IndexBase {

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

    private final StandardTable table;
    private final String mapName;
    private final TransactionMap<Value, Value> dataMap;
    private long lastKey;
    private int mainIndexColumn = -1;

    public StandardPrimaryIndex(Session session, StandardTable table) {
        IndexColumn[] columns = IndexColumn.wrap(table.getColumns());
        initIndexBase(table, table.getId(), table.getName() + "_DATA", columns, IndexType.createScan(true));

        this.table = table;
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(database.getCompareMode(), database, sortTypes);
        mapName = "table." + table.getName() + "." + getId();

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();
        dataMap = transactionEngine.beginTransaction(false).openMap(mapName, table.getMapType(), keyType, valueType,
                storage);

        // TODO
        // Fix bug when creating lots of temporary tables, where we could run out of transaction IDs
        session.commit(false);
        if (!table.isPersistData()) {
            dataMap.setVolatile(true);
        }
        Value k = dataMap.lastKey();
        lastKey = k == null ? 0 : k.getLong();
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

        TransactionMap<Value, Value> map = getMap(session);
        Value key = ValueLong.get(row.getKey());
        Value old = map.getLatest(key);
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
            map.put(key, ValueArray.get(row.getValueList()));
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
        lastKey = Math.max(lastKey, row.getKey());
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
        TransactionMap<Value, Value> map = getMap(session);
        try {
            Value old = map.remove(ValueLong.get(row.getKey()));
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ValueLong min, max;
        if (first == null) {
            min = MIN;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            ValueLong v = (ValueLong) first.getValue(mainIndexColumn);
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
            ValueLong v = (ValueLong) last.getValue(mainIndexColumn);
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        TransactionMap<Value, Value> map = getMap(session);
        return new StandardPrimaryIndexCursor(map.entryIterator(min), max);
    }

    @Override
    public StandardTable getTable() {
        return table;
    }

    @Override
    public Row getRow(Session session, long key) {
        TransactionMap<Value, Value> map = getMap(session);
        Value v = map.get(ValueLong.get(key));
        ValueArray array = (ValueArray) v;
        Row row = new Row(array.getList(), 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        try {
            long cost = 10 * (dataMap.sizeAsLongMax() + Constants.COST_ROW_OFFSET);
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
        TransactionMap<Value, Value> map = getMap(session);
        if (!map.isClosed()) {
            map.removeMap();
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (table.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueLong v = (ValueLong) (first ? map.firstKey() : map.lastKey());
        if (v == null) {
            return new StandardPrimaryIndexCursor(Collections.<Entry<Value, Value>> emptyList().iterator(), null);
        }
        Value value = map.get(v);
        Entry<Value, Value> e = new DataUtils.MapEntry<Value, Value>(v, value);
        List<Entry<Value, Value>> list = Arrays.asList(e);
        StandardPrimaryIndexCursor c = new StandardPrimaryIndexCursor(list.iterator(), v);
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return dataMap.sizeAsLongMax();
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
        // TODO estimate disk space usage
        return 0;
    }

    public String getMapName() {
        return mapName;
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
    Cursor find(Session session, ValueLong first, ValueLong last) {
        TransactionMap<Value, Value> map = getMap(session);
        return new StandardPrimaryIndexCursor(map.entryIterator(first), last);
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<Value, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        return dataMap.getInstance(session.getTransaction(), Long.MAX_VALUE);
    }

    boolean isInMemory() {
        return dataMap.isInMemory();
    }

    /**
     * A cursor.
     */
    private static class StandardPrimaryIndexCursor implements Cursor {

        private final Iterator<Entry<Value, Value>> it;
        private final ValueLong last;
        private Entry<Value, Value> current;
        private Row row;

        public StandardPrimaryIndexCursor(Iterator<Entry<Value, Value>> it, ValueLong last) {
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    ValueArray array = (ValueArray) current.getValue();
                    row = new Row(array.getList(), 0);
                    row.setKey(current.getKey().getLong());
                }
            }
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
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

}
