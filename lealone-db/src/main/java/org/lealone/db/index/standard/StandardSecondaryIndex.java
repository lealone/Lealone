/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index.standard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.EmptyCursor;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.StandardTable;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.storage.Storage;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardSecondaryIndex extends StandardIndex {

    private final StandardTable table;
    private final String mapName;
    private final int keyColumns;
    private final TransactionMap<Value, Value> dataMap;

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

    private TransactionMap<Value, Value> openMap(ServerSession session, String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;

        ValueDataType keyType;
        if (indexType.isUnique())
            keyType = new UniqueKeyDataType(database, database.getCompareMode(), sortTypes);
        else
            keyType = new ValueDataType(database, database.getCompareMode(), sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionMap<Value, Value> map = session.getTransaction().openMap(mapName, keyType, valueType, storage,
                table.getParameters());
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
        final TransactionMap<Value, Value> map = getMap(session);
        final ValueArray array = convertToKey(row);

        AsyncCallback<Integer> ac = new AsyncCallback<>();
        map.addIfAbsent(array, ValueNull.INSTANCE).onComplete(ar -> {
            if (ar.isFailed()) {
                // 违反了唯一性，
                // 或者byte/short/int/long类型的primary key + 约束字段构成的索引
                // 因为StandardPrimaryIndex和StandardSecondaryIndex的add是异步并行执行的，
                // 有可能先跑StandardSecondaryIndex先，所以可能得到相同的索引key，
                // 这时StandardPrimaryIndex和StandardSecondaryIndex都会检测到重复key的异常。
                DbException e = getDuplicateKeyException(array.toString());
                ac.setAsyncResult(e);
            } else {
                ac.setAsyncResult(ar);
            }
        });
        return ac;
    }

    @Override
    public Future<Integer> update(ServerSession session, Row oldRow, Row newRow, List<Column> updateColumns,
            boolean isLockedBySelf) {
        // 只有索引字段被更新时才更新索引
        for (Column c : columns) {
            if (updateColumns.contains(c)) {
                return super.update(session, oldRow, newRow, updateColumns, isLockedBySelf);
            }
        }
        return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueArray array = convertToKey(row);
        Object oldTransactionalValue = map.getTransactionalValue(array);
        if (!isLockedBySelf && map.isLocked(oldTransactionalValue, null))
            return Future.succeededFuture(
                    map.addWaitingTransaction(ValueLong.get(row.getKey()), oldTransactionalValue, null));
        else
            return Future.succeededFuture(map.tryRemove(array, oldTransactionalValue, isLockedBySelf));
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        return new StandardSecondaryIndexCursor(session, getMap(session).keyIterator(min), last);
    }

    private ValueArray convertToKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType());
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return ValueArray.get(array);
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
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        Value key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return EmptyCursor.INSTANCE;
            }
            if (((ValueArray) key).getList()[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        ArrayList<Value> list = new ArrayList<>(1);
        list.add(key);
        StandardSecondaryIndexCursor cursor = new StandardSecondaryIndexCursor(session, list.iterator(), null);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean supportsDistinctQuery() {
        return true;
    }

    @Override
    public Cursor findDistinct(ServerSession session, SearchRow first, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        return new StandardSecondaryIndexDistinctCursor(session, min, last);
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
        TransactionMap<Value, Value> map = getMap(session);
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
    private TransactionMap<Value, Value> getMap(ServerSession session) {
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

    @Override
    public void addRowsToBuffer(ServerSession session, List<Row> rows, String bufferName) {
        TransactionMap<Value, Value> map = openMap(session, bufferName);
        for (Row row : rows) {
            ValueArray key = convertToKey(row);
            map.put(key, ValueNull.INSTANCE);
        }
    }

    @Override
    public void addBufferedRows(ServerSession session, List<String> bufferNames) {
        ArrayList<String> mapNames = new ArrayList<>(bufferNames);
        final CompareMode compareMode = database.getCompareMode();
        /**
         * A source of values.
         */
        class Source implements Comparable<Source> {
            Value value;
            Iterator<Value> next;
            int sourceId;

            @Override
            public int compareTo(Source o) {
                int comp = value.compareTo(o.value, compareMode);
                if (comp == 0) {
                    comp = sourceId - o.sourceId;
                }
                return comp;
            }
        }
        TreeSet<Source> sources = new TreeSet<Source>();
        for (int i = 0; i < bufferNames.size(); i++) {
            TransactionMap<Value, Value> map = openMap(session, bufferNames.get(i));
            Iterator<Value> it = map.keyIterator(null, true);
            if (it.hasNext()) {
                Source s = new Source();
                s.value = it.next();
                s.next = it;
                s.sourceId = i;
                sources.add(s);
            }
        }
        try {
            while (true) {
                Source s = sources.first();
                Value v = s.value;

                if (indexType.isUnique()) {
                    Value[] array = ((ValueArray) v).getList();
                    // don't change the original value
                    array = array.clone();
                    array[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
                    ValueArray unique = ValueArray.get(array);
                    SearchRow row = convertToSearchRow((ValueArray) v);
                    checkUnique(row, dataMap, unique);
                }

                dataMap.putCommitted(v, ValueNull.INSTANCE);

                Iterator<Value> it = s.next;
                if (!it.hasNext()) {
                    sources.remove(s);
                    if (sources.isEmpty()) {
                        break;
                    }
                } else {
                    Value nextValue = it.next();
                    sources.remove(s);
                    s.value = nextValue;
                    sources.add(s);
                }
            }
        } finally {
            for (String tempMapName : mapNames) {
                TransactionMap<Value, Value> map = openMap(session, tempMapName);
                map.remove();
            }
        }
    }

    private void checkUnique(SearchRow row, TransactionMap<Value, Value> map, ValueArray unique) {
        Iterator<Value> it = map.keyIterator(unique, true);
        while (it.hasNext()) {
            ValueArray k = (ValueArray) it.next();
            SearchRow r2 = convertToSearchRow(k);
            if (compareRows(row, r2) != 0) {
                break;
            }
            if (map.get(k) != null) {
                if (!containsNullAndAllowMultipleNull(r2)) {
                    throw getDuplicateKeyException(k.toString());
                }
            }
        }
    }

    /**
     * Convert array of values to a SearchRow.
     *
     * @param array the index key
     * @return the row
     */
    private SearchRow convertToSearchRow(ValueArray key) {
        Value[] array = key.getList();
        int len = array.length - 1;
        SearchRow searchRow = table.getTemplateRow();
        searchRow.setKey((array[len]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < len; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    /**
     * A cursor.
     */
    private class StandardSecondaryIndexCursor implements Cursor {

        private final ServerSession session;
        private final Iterator<Value> it;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        public StandardSecondaryIndexCursor(ServerSession session, Iterator<Value> it, SearchRow last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            return get(null);
        }

        @Override
        public Row get(int[] columnIndexes) {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = table.getRow(session, r.getKey(), columnIndexes);
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            if (searchRow == null) {
                if (current != null) {
                    searchRow = convertToSearchRow((ValueArray) current);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            searchRow = null;
            if (current != null) {
                if (last != null && compareRows(getSearchRow(), last) > 0) {
                    searchRow = null;
                    current = null;
                }
            }
            row = null;
            return current != null;
        }
    }

    private class StandardSecondaryIndexDistinctCursor implements Cursor {

        private final TransactionMap<Value, Value> map;
        private final ServerSession session;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        private ValueArray oldKey;

        public StandardSecondaryIndexDistinctCursor(ServerSession session, ValueArray min, SearchRow last) {
            this.map = getMap(session);
            this.session = session;
            this.last = last;
            this.current = min;
        }

        @Override
        public Row get() {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = table.getRow(session, r.getKey());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            if (searchRow == null) {
                if (current != null) {
                    searchRow = convertToSearchRow((ValueArray) current);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            Value newKey = map.higherKey(oldKey); // oldKey从null开始，此时返回第一个元素
            current = newKey;
            searchRow = null;
            row = null;
            if (newKey != null) {
                if (last != null && compareRows(getSearchRow(), last) > 0) {
                    searchRow = null;
                    current = null;
                }
            }
            if (current != null) {
                Value[] currentValues = ((ValueArray) current).getList();
                if (oldKey == null) {
                    Value[] oldValues = new Value[keyColumns];
                    System.arraycopy(currentValues, 0, oldValues, 0, keyColumns - 1);
                    oldValues[keyColumns - 1] = ValueLong.get(Long.MAX_VALUE);
                    oldKey = ValueArray.get(oldValues);
                } else {
                    Value[] oldValues = oldKey.getList();
                    for (int i = 0, size = keyColumns - 1; i < size; i++)
                        oldValues[i] = currentValues[i];
                }
                return true;
            }
            return false;
        }
    }
}
