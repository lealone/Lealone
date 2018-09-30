/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Constants;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.StandardTable;
import org.lealone.db.table.TableAlterHistoryRecord;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.storage.PageKey;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
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

    private final StandardTable table;
    private final String mapName;
    private final TransactionMap<Value, VersionedValue> dataMap;
    private int mainIndexColumn = -1;

    public StandardPrimaryIndex(ServerSession session, StandardTable table) {
        super(table, table.getId(), table.getName() + "_DATA", IndexType.createScan(),
                IndexColumn.wrap(table.getColumns()));
        this.table = table;
        mapName = table.getMapNameForTable(getId());
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(database, database.getCompareMode(), sortTypes);
        VersionedValueType vvType = new VersionedValueType(valueType);

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();

        String initReplicationEndpoints = null;
        String replicationName = session.getReplicationName();
        if (replicationName != null) {
            int pos = replicationName.indexOf('@');
            if (pos != -1) {
                initReplicationEndpoints = replicationName.substring(0, pos);
            }
        }

        // session.isShardingMode()是针对当前session的，如果是SystemSession，就算数据库是ShardingMode，也不管它
        Transaction t = transactionEngine.beginTransaction(false, session.isShardingMode());
        dataMap = t.openMap(mapName, table.getMapType(), keyType, vvType, storage,
                session.getDatabase().isShardingMode(), initReplicationEndpoints);
        transactionEngine.addTransactionMap(dataMap);
        t.commit(); // 避免产生内部未提交的事务
    }

    @Override
    public StandardTable getTable() {
        return table;
    }

    public String getMapName() {
        return mapName;
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
    public void close(ServerSession session) {
        // ok
    }

    @Override
    public void add(ServerSession session, Row row) {
        // insert新记录并且由系统自动增加rowKey时，不用每次都调用一次map.get
        // update原有记录row.getKey()不为0，所以依然要调用map.get
        boolean checkDuplicateKey = true;
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                checkDuplicateKey = false;
            }
        } else {
            long k = row.getValue(mainIndexColumn).getLong();
            row.setKey(k);
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

        TransactionMap<Value, VersionedValue> map = getMap(session);
        VersionedValue value = new VersionedValue(row.getVersion(), ValueArray.get(row.getValueList()));
        Value key;
        if (checkDuplicateKey) {
            key = ValueLong.get(row.getKey());
            VersionedValue old = map.get(key);
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
                map.put(key, value);
            } catch (IllegalStateException e) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
            }
        } else {
            key = map.append(value);
            row.setKey(key.getLong());
        }
        session.setLastRow(row);
        session.setLastIndex(this);
    }

    @Override
    public void remove(ServerSession session, Row row) {
        if (table.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        TransactionMap<Value, VersionedValue> map = getMap(session);
        try {
            VersionedValue old = map.remove(ValueLong.get(row.getKey()));
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        ValueLong[] minAndMaxValues = getMinAndMaxValues(first, last);
        return new StandardPrimaryIndexCursor(session, table, this, getMap(session).entryIterator(minAndMaxValues[0]),
                minAndMaxValues[1]);
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        VersionedValue v = getMap(session).get(ValueLong.get(key));
        ValueArray array = v.value;
        Row row = new Row(array.getList(), 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        try {
            long cost = 10 * (dataMap.rawSize() + Constants.COST_ROW_OFFSET);
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
    public void remove(ServerSession session) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        if (!map.isClosed()) {
            map.remove();
        }
    }

    @Override
    public void truncate(ServerSession session) {
        if (table.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        getMap(session).clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        ValueLong v = (ValueLong) (first ? map.firstKey() : map.lastKey());
        if (v == null) {
            return new StandardPrimaryIndexCursor(session, table, this,
                    Collections.<Entry<Value, VersionedValue>> emptyList().iterator(), null);
        }
        VersionedValue value = map.get(v);
        Entry<Value, VersionedValue> e = new DataUtils.MapEntry<Value, VersionedValue>(v, value);
        List<Entry<Value, VersionedValue>> list = Arrays.asList(e);
        StandardPrimaryIndexCursor c = new StandardPrimaryIndexCursor(session, table, this, list.iterator(), v);
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return getMap(session).sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return dataMap.rawSize();
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
        return dataMap.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return dataMap.getMemorySpaceUsed();
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
    Cursor find(ServerSession session, ValueLong first, ValueLong last) {
        return new StandardPrimaryIndexCursor(session, table, this, getMap(session).entryIterator(first), last);
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
    TransactionMap<Value, VersionedValue> getMap(ServerSession session) {
        if (session == null) {
            return dataMap;
        }
        return dataMap.getInstance(session.getTransaction());
    }

    boolean isInMemory() {
        return dataMap.isInMemory();
    }

    @Override
    public StorageMap<? extends Object, ? extends Object> getStorageMap() {
        return dataMap;
    }

    private ValueLong[] getMinAndMaxValues(SearchRow first, SearchRow last) {
        ValueLong min, max;
        if (first == null) {
            min = MIN;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            Value value = first.getValue(mainIndexColumn);
            ValueLong v;
            if (value instanceof ValueLong)
                v = (ValueLong) value;
            else
                v = ValueLong.get(value.getLong());
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
            Value value = first.getValue(mainIndexColumn);
            ValueLong v;
            if (value instanceof ValueLong)
                v = (ValueLong) value;
            else
                v = ValueLong.get(value.getLong());
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        return new ValueLong[] { min, max };
    }

    @Override
    public Map<String, List<PageKey>> getEndpointToPageKeyMap(ServerSession session, SearchRow first, SearchRow last) {
        ValueLong[] minAndMaxValues = getMinAndMaxValues(first, last);
        return getMap(session).getEndpointToPageKeyMap(session, minAndMaxValues[0], minAndMaxValues[1]);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last, List<PageKey> pageKeys) {
        ValueLong[] minAndMaxValues = getMinAndMaxValues(first, last);
        return new StandardPrimaryIndexCursor(session, table, this,
                getMap(session).entryIterator(pageKeys, minAndMaxValues[0]), minAndMaxValues[1]);
    }

    /**
     * A cursor.
     */
    private static class StandardPrimaryIndexCursor implements Cursor {

        private final ServerSession session;
        private final StandardTable table;
        private final StandardPrimaryIndex index;
        private final Iterator<Entry<Value, VersionedValue>> it;
        private final ValueLong last;
        private Entry<Value, VersionedValue> current;
        private Row row;

        public StandardPrimaryIndexCursor(ServerSession session, StandardTable table, StandardPrimaryIndex index,
                Iterator<Entry<Value, VersionedValue>> it, ValueLong last) {
            this.session = session;
            this.table = table;
            this.index = index;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    VersionedValue value = current.getValue();
                    Value[] data = value.value.getList();
                    int version = value.vertion;
                    row = new Row(data, 0);
                    row.setKey(current.getKey().getLong());
                    row.setVersion(version);

                    if (table.getVersion() != version) {
                        ArrayList<TableAlterHistoryRecord> records = table.getDatabase()
                                .getTableAlterHistoryRecord(table.getId(), version, table.getVersion());
                        Value[] newValues = data;
                        for (TableAlterHistoryRecord record : records) {
                            newValues = record.redo(session, newValues);
                        }
                        if (newValues != data) {
                            index.remove(session, row);
                            row = new Row(newValues, 0);
                            row.setKey(current.getKey().getLong());
                            row.setVersion(table.getVersion());
                            index.add(session, row);
                        }
                    }
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
