/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index.standard;

import java.util.HashMap;
import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.StandardTable;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueEnum;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageSetting;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;
import com.lealone.transaction.TransactionMapCursor;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardSecondaryIndex extends StandardIndex {

    private final StandardTable table;
    private final String mapName;
    private final int keyColumns;
    private final TransactionMap<IndexKey, Value> dataMap;

    public StandardSecondaryIndex(ServerSession session, StandardTable table, int id, String indexName,
            IndexType indexType, IndexColumn[] indexColumns) {
        super(table, id, indexName, indexType, indexColumns);
        this.table = table;
        mapName = table.getMapNameForIndex(id);
        if (!database.isStarting()) {
            checkIndexColumnTypes(indexColumns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = indexColumns.length + 1;

        dataMap = openMap(session, mapName);
    }

    private TransactionMap<IndexKey, Value> openMap(ServerSession session, String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;

        IndexKeyType keyType;
        if (indexType.isUnique())
            keyType = new UniqueKeyType(database, database.getCompareMode(), sortTypes);
        else
            keyType = new IndexKeyType(database, database.getCompareMode(), sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);

        Storage storage = database.getStorage(table.getStorageEngine());
        Map<String, String> parameters = table.getParameters();
        if (!table.isPersistIndexes()) {
            parameters = new HashMap<>(parameters);
            parameters.put(StorageSetting.IN_MEMORY.name(), "1");
        }
        TransactionMap<IndexKey, Value> map = session.getTransaction().openMap(mapName, keyType,
                valueType, storage, parameters);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.getInternalError("Incompatible key type");
        }
        return map;
    }

    @Override
    public StandardTable getTable() {
        return table;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public Future<Integer> add(ServerSession session, Row row) {
        final TransactionMap<IndexKey, Value> map = getMap(session);
        final IndexKey key = convertToKey(row);

        AsyncCallback<Integer> ac = session.createCallback();
        map.addIfAbsent(key, ValueNull.INSTANCE).onComplete(ar -> {
            if (ar.isFailed()) {
                // 违反了唯一性，
                // 或者byte/short/int/long类型的primary key + 约束字段构成的索引
                // 因为StandardPrimaryIndex和StandardSecondaryIndex的add是异步并行执行的，
                // 有可能先跑StandardSecondaryIndex先，所以可能得到相同的索引key，
                // 这时StandardPrimaryIndex和StandardSecondaryIndex都会检测到重复key的异常。
                DbException e = getDuplicateKeyException(key.toString());
                ac.setAsyncResult(e);
            } else {
                ac.setAsyncResult(ar);
            }
        });
        return ac;
    }

    @Override
    public Future<Integer> update(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf) {
        // 只有索引字段被更新时才更新索引
        for (Column c : columns) {
            if (StandardPrimaryIndex.containsColumn(updateColumns, c)) {
                return super.update(session, oldRow, newRow, updateColumns, isLockedBySelf);
            }
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        TransactionMap<IndexKey, Value> map = getMap(session);
        IndexKey key = convertToKey(row);
        Object tv = map.getTransactionalValue(key);
        if (!isLockedBySelf && map.isLocked(tv, null))
            return Future.succeededFuture(map.addWaitingTransaction(key, tv));
        else
            return Future.succeededFuture(map.tryRemove(key, tv, isLockedBySelf));
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        IndexKey min = convertToKey(first);
        if (min != null) {
            min.columns[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        return new StandardSecondaryIndexRegularCursor(session, getMap(session).cursor(min), last);
    }

    private IndexKey convertToKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                if (c.isEnumType()) {
                    try {
                        array[i] = c.convert(v);
                    } catch (Throwable t) {
                        array[i] = ValueEnum.get(-1);
                    }
                } else {
                    array[i] = v.convertTo(c.getType());
                }
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return new IndexKey(array);
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.getRawSize(), sortOrder);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public SearchRow findFirstOrLast(ServerSession session, boolean first) {
        TransactionMap<IndexKey, Value> map = getMap(session);
        IndexKey key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return null;
            }
            if (key.columns[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        return convertToSearchRow(key);
    }

    @Override
    public boolean supportsDistinctQuery() {
        return true;
    }

    @Override
    public Cursor findDistinct(ServerSession session) {
        return new StandardSecondaryIndexDistinctCursor(session);
    }

    @Override
    public long getRowCount(ServerSession session) {
        return getMap(session).size();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.getRawSize();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        return dataMap.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return dataMap.getMemorySpaceUsed();
    }

    @Override
    public void remove(ServerSession session) {
        TransactionMap<IndexKey, Value> map = getMap(session);
        if (!map.isClosed()) {
            map.remove();
        }
    }

    @Override
    public void truncate(ServerSession session) {
        getMap(session).clear();
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    private TransactionMap<IndexKey, Value> getMap(ServerSession session) {
        if (session == null) {
            return dataMap;
        }
        return dataMap.getInstance(session.getTransaction());
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.getRawSize() == 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public boolean isInMemory() {
        return dataMap.isInMemory();
    }

    /**
     * Convert array of values to a SearchRow.
     *
     * @param array the index key
     * @return the row
     */
    private SearchRow convertToSearchRow(IndexKey key) {
        Value[] array = key.columns;
        int len = array.length - 1;
        SearchRow searchRow = table.getTemplateRow();
        searchRow.setKey((array[len]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < len; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            if (c.isEnumType())
                v = c.convert(v);
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    private abstract class StandardSecondaryIndexCursor implements Cursor {

        private final ServerSession session;
        private SearchRow searchRow;
        private Row row;

        public StandardSecondaryIndexCursor(ServerSession session) {
            this.session = session;
        }

        @Override
        public Row get() {
            return get(null);
        }

        @Override
        public Row get(int[] columnIndexes) {
            if (row == null && searchRow != null) {
                row = table.getRow(session, searchRow.getKey(), columnIndexes);
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return searchRow;
        }

        @Override
        public boolean next() {
            searchRow = nextSearchRow();
            row = null; // 延迟构建
            return searchRow != null;
        }

        protected SearchRow createSearchRow(IndexKey key) {
            if (key == null)
                return null;
            else
                return convertToSearchRow(key);
        }

        protected abstract SearchRow nextSearchRow();
    }

    private class StandardSecondaryIndexRegularCursor extends StandardSecondaryIndexCursor {

        private final TransactionMapCursor<IndexKey, ?> tmCursor;
        private final SearchRow last;

        public StandardSecondaryIndexRegularCursor(ServerSession session,
                TransactionMapCursor<IndexKey, ?> tmCursor, SearchRow last) {
            super(session);
            this.tmCursor = tmCursor;
            this.last = last;
        }

        @Override
        protected SearchRow nextSearchRow() {
            SearchRow searchRow;
            if (tmCursor.next()) {
                IndexKey current = tmCursor.getKey();
                searchRow = createSearchRow(current);
                if (searchRow != null && last != null && compareRows(searchRow, last) > 0) {
                    searchRow = null;
                }
            } else {
                searchRow = null;
            }
            return searchRow;
        }
    }

    private class StandardSecondaryIndexDistinctCursor extends StandardSecondaryIndexCursor {

        private final TransactionMap<IndexKey, Value> map;
        private IndexKey oldKey;

        public StandardSecondaryIndexDistinctCursor(ServerSession session) {
            super(session);
            this.map = getMap(session);
        }

        @Override
        protected SearchRow nextSearchRow() {
            IndexKey current = map.higherKey(oldKey); // oldKey从null开始，此时返回第一个元素
            if (current != null) {
                Value[] currentValues = current.columns;
                if (oldKey == null) {
                    Value[] oldValues = new Value[keyColumns];
                    System.arraycopy(currentValues, 0, oldValues, 0, keyColumns - 1);
                    oldValues[keyColumns - 1] = ValueLong.get(Long.MAX_VALUE);
                    oldKey = new IndexKey(oldValues);
                } else {
                    Value[] oldValues = oldKey.columns;
                    for (int i = 0, size = keyColumns - 1; i < size; i++)
                        oldValues[i] = currentValues[i];
                }
            }
            return createSearchRow(current);
        }
    }
}
