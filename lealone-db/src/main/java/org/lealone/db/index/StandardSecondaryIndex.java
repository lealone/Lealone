/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.New;
import org.lealone.common.value.CompareMode;
import org.lealone.common.value.Value;
import org.lealone.common.value.ValueArray;
import org.lealone.common.value.ValueLong;
import org.lealone.common.value.ValueNull;
import org.lealone.db.Database;
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

public class StandardSecondaryIndex extends IndexBase implements StandardIndex {

    private final StandardTable table;
    private final int keyColumns;
    private final String mapName;
    private final TransactionMap<Value, Value> dataMap;

    public StandardSecondaryIndex(Session session, StandardTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        Database db = session.getDatabase();
        this.table = table;
        initIndexBase(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(db.getCompareMode(), db, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);
        mapName = StandardTable.getMapNameForIndex(getId());

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();
        dataMap = transactionEngine.beginTransaction(false).openMap(mapName, table.getMapType(), keyType, valueType,
                storage);

        // TODO
        // Fix bug when creating lots of temporary tables, where we could run out of transaction IDs
        session.commit(false);
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
    }

    @Override
    public void addRowsToBuffer(Session session, List<Row> rows, String bufferName) {
        TransactionMap<Value, Value> map = openMap(session, bufferName);
        for (Row row : rows) {
            ValueArray key = convertToKey(row);
            map.put(key, ValueNull.INSTANCE);
        }
    }

    @Override
    public void addBufferedRows(Session session, List<String> bufferNames) {
        ArrayList<String> mapNames = New.arrayList(bufferNames);
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
            Iterator<Value> it = map.keyIterator(null);
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

    // TODO 不考虑事务
    private TransactionMap<Value, Value> openMap(Session session, String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(database.getCompareMode(), database, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();
        TransactionMap<Value, Value> map = transactionEngine.beginTransaction(false).openMap(mapName,
                table.getMapType(), keyType, valueType, storage);

        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueArray array = convertToKey(row);
        ValueArray unique = null;
        if (indexType.isUnique()) {
            // this will detect committed entries only
            unique = convertToKey(row);
            unique.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
            checkUnique(row, map, unique);
        }
        try {
            map.put(array, ValueNull.INSTANCE);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
        if (indexType.isUnique()) {
            Iterator<Value> it = map.keyIterator(unique, true);
            while (it.hasNext()) {
                ValueArray k = (ValueArray) it.next();
                SearchRow r2 = convertToSearchRow(k);
                if (compareRows(row, r2) != 0) {
                    break;
                }
                if (containsNullAndAllowMultipleNull(r2)) {
                    // this is allowed
                    continue;
                }
                if (map.isSameTransaction(k)) {
                    continue;
                }
                if (map.get(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
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

    @Override
    public void remove(Session session, Row row) {
        ValueArray array = convertToKey(row);
        TransactionMap<Value, Value> map = getMap(session);
        try {
            Value old = map.remove(array);
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, e, table.getName());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        TransactionMap<Value, Value> map = getMap(session);
        return new StandardSecondaryIndexCursor(session, map.keyIterator(min), last);
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

    /**
     * Convert array of values to a SearchRow.
     *
     * @param array the index key
     * @return the row
     */
    SearchRow convertToSearchRow(ValueArray key) {
        Value[] array = key.getList();
        SearchRow searchRow = table.getTemplateRow();
        searchRow.setKey((array[array.length - 1]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < array.length - 1; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    @Override
    public StandardTable getTable() {
        return table;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.rawSize(), filter, sortOrder);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (!map.isClosed()) {
            map.remove();
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        Value key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return new StandardSecondaryIndexCursor(session, Collections.<Value> emptyList().iterator(), null);
            }
            if (((ValueArray) key).getList()[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        ArrayList<Value> list = New.arrayList();
        list.add(key);
        StandardSecondaryIndexCursor cursor = new StandardSecondaryIndexCursor(session, list.iterator(), null);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.rawSize() == 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.rawSize();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean supportsDistinctQuery() {
        return true;
    }

    @Override
    public Cursor findDistinct(Session session, SearchRow first, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        return new StandardSecondaryIndexDistinctCursor(session, min, last);
    }

    @Override
    public void checkRename() {
        // ok
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
        return dataMap.getInstance(session.getTransaction());
    }

    @Override
    public boolean isInMemory() {
        return dataMap.isInMemory();
    }

    /**
     * A cursor.
     */
    private class StandardSecondaryIndexCursor implements Cursor {

        private final Session session;
        private final Iterator<Value> it;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        public StandardSecondaryIndexCursor(Session session, Iterator<Value> it, SearchRow last) {
            this.session = session;
            this.it = it;
            this.last = last;
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

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

    private class StandardSecondaryIndexDistinctCursor implements Cursor {
        private final TransactionMap<Value, Value> map;
        private final Session session;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        public StandardSecondaryIndexDistinctCursor(Session session, ValueArray min, SearchRow last) {
            this.session = session;
            this.last = last;
            map = getMap(session);
            current = min;
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
            current = map.higherKey(current);
            searchRow = null;
            if (current != null) {
                ((ValueArray) current).getList()[keyColumns - 1] = ValueLong.get(Long.MAX_VALUE);
                if (last != null && compareRows(getSearchRow(), last) > 0) {
                    searchRow = null;
                    current = null;
                }
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
