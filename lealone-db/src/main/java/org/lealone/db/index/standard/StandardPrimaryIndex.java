/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index.standard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.EmptyCursor;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.lock.DbObjectLockImpl;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.StandardTable;
import org.lealone.db.table.TableAlterHistoryRecord;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.page.PageKey;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.TransactionMapEntry;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardPrimaryIndex extends StandardIndex {

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
        VersionedValueType vvType = new VersionedValueType(valueType, columns.length);

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();

        // session.getRunMode()是针对当前session的，如果是SystemSession，就算数据库是ShardingMode，也不管它
        Transaction t = transactionEngine.beginTransaction(false, session.getRunMode());
        dataMap = t.openMap(mapName, keyType, vvType, storage, table.getParameters());
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
    public Future<Integer> add(ServerSession session, Row row) {
        // 由系统自动增加rowKey并且应用没有指定rowKey时用append来实现(不需要检测rowKey是否重复)，其他的用addIfAbsent实现
        boolean checkDuplicateKey = true;
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                checkDuplicateKey = false;
            }
        } else {
            long k = row.getValue(mainIndexColumn).getLong();
            row.setKey(k);
        }

        if (table.containsLargeObject()) {
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

        AsyncCallback<Integer> ac = new AsyncCallback<>();
        TransactionMap<Value, VersionedValue> map = getMap(session);
        VersionedValue value = new VersionedValue(row.getVersion(), ValueArray.get(row.getValueList()));
        if (checkDuplicateKey) {
            Value key = ValueLong.get(row.getKey());
            map.addIfAbsent(key, value).onComplete(ar -> {
                if (ar.isFailed()) {
                    String sql = "PRIMARY KEY ON " + table.getSQL();
                    if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                        sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
                    }
                    DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
                    ac.setAsyncResult(e);
                } else {
                    ac.setAsyncResult(ar);
                }
            });
        } else {
            map.append(value, ar -> {
                if (ar.isSucceeded()) {
                    row.setKey(ar.getResult().getLong());
                    session.setLastRow(row);
                }
                ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
            });
        }
        return ac;
    }

    static boolean containsColumn(int[] updateColumns, Column c) {
        int cId = c.getColumnId();
        for (int i = 0; i < updateColumns.length; i++) {
            if (updateColumns[i] == cId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Future<Integer> update(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf) {
        if (mainIndexColumn != -1) {
            if (containsColumn(updateColumns, columns[mainIndexColumn])) {
                Value oldKey = oldRow.getValue(mainIndexColumn);
                Value newKey = newRow.getValue(mainIndexColumn);
                // 修改了主键字段并且新值与旧值不同时才会册除原有的并增加新的，因为这种场景下性能慢一些
                if (!oldKey.equals(newKey)) {
                    return super.update(session, oldRow, newRow, updateColumns, isLockedBySelf);
                } else if (updateColumns.length == 1) { // 新值与旧值相同，并且只更新主键时什么都不用做
                    return Future.succeededFuture(Transaction.OPERATION_COMPLETE);
                }
            }
        }
        TransactionMap<Value, VersionedValue> map = getMap(session);
        if (!isLockedBySelf && map.isLocked(oldRow.getTValue(), updateColumns))
            return Future
                    .succeededFuture(map.addWaitingTransaction(ValueLong.get(oldRow.getKey()), oldRow.getTValue()));

        if (table.containsLargeObject()) {
            for (int i = 0, len = newRow.getColumnCount(); i < len; i++) {
                Value v = newRow.getValue(i);
                Value v2 = v.link(database, getId());
                if (v2.isLinked()) {
                    session.unlinkAtCommitStop(v2);
                }
                if (v != v2) {
                    newRow.setValue(i, v2);
                }
            }
            for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                Value v = oldRow.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        VersionedValue newValue = new VersionedValue(newRow.getVersion(), ValueArray.get(newRow.getValueList()));
        Value key = ValueLong.get(newRow.getKey());
        int ret = map.tryUpdate(key, newValue, updateColumns, oldRow.getTValue(), isLockedBySelf);
        session.setLastRow(newRow);
        return Future.succeededFuture(ret);
    }

    @Override
    public Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        Value key = ValueLong.get(row.getKey());
        Object tv = row.getTValue();
        TransactionMap<Value, VersionedValue> map = getMap(session);

        if (!isLockedBySelf && map.isLocked(tv, null))
            return Future.succeededFuture(map.addWaitingTransaction(key, tv));

        if (table.containsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        return Future.succeededFuture(map.tryRemove(key, tv, isLockedBySelf));
    }

    @Override
    public boolean tryLock(ServerSession session, Row row, int[] lockColumns, boolean isForUpdate) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        return map.tryLock(ValueLong.get(row.getKey()), row.getTValue(), lockColumns, isForUpdate);
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        return find(session, CursorParameters.create(first, last));
    }

    @Override
    public Cursor find(ServerSession session, CursorParameters<SearchRow> parameters) {
        ValueLong from = getPK(parameters.from);
        ValueLong to = getPK(parameters.to);
        CursorParameters<Value> newParameters = parameters.copy(from, to);
        return new StandardPrimaryIndexCursor(session, table, this, getMap(session).entryIterator(newParameters), to);
    }

    @Override
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        ValueLong v = (ValueLong) (first ? map.firstKey() : map.lastKey());
        if (v == null) {
            return EmptyCursor.INSTANCE;
        }
        VersionedValue value = map.get(v);
        TransactionMapEntry<Value, VersionedValue> e = new TransactionMapEntry<>(v, value);
        List<TransactionMapEntry<Value, VersionedValue>> list = Arrays.asList(e);
        StandardPrimaryIndexCursor c = new StandardPrimaryIndexCursor(session, table, this, list.iterator(), v);
        c.next();
        return c;
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        return getRow(session, key, null);
    }

    public Row getRow(ServerSession session, long key, int[] columnIndexes) {
        Object[] valueAndRef = getMap(session).getValueAndRef(ValueLong.get(key), columnIndexes);
        VersionedValue v = (VersionedValue) valueAndRef[0];
        ValueArray array = v.value;
        array = v.value;
        Row row = new Row(array.getList(), 0);
        row.setKey(key);
        row.setVersion(v.version);
        row.setTValue(valueAndRef[1]);
        return row;
    }

    public Row getRow(ServerSession session, long key, Object oldTValue) {
        Object value = getMap(session).getValue(oldTValue);
        // 已经删除了
        if (value == null)
            return null;
        VersionedValue v = (VersionedValue) value;
        ValueArray array = v.value;
        array = v.value;
        Row row = new Row(array.getList(), 0);
        row.setKey(key);
        row.setVersion(v.version);
        row.setTValue(oldTValue);
        return row;
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        try {
            return 10 * (dataMap.getRawSize() + Constants.COST_ROW_OFFSET);
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
        // 内存数据库没有LobStorage
        if (table.containsLargeObject() && database.getLobStorage() != null) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        getMap(session).clear();
    }

    @Override
    public long getRowCount(ServerSession session) {
        return getMap(session).size();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return dataMap.getRawSize();
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
            throw DbException.getInternalError(row.toString());
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
    private TransactionMap<Value, VersionedValue> getMap(ServerSession session) {
        if (session == null) {
            return dataMap;
        }
        return dataMap.getInstance(session.getTransaction());
    }

    @Override
    public boolean isInMemory() {
        return dataMap.isInMemory();
    }

    private ValueLong getPK(SearchRow row) {
        ValueLong pk;
        if (row == null) {
            pk = null; // 设为null，避免不必要的比较
        } else if (mainIndexColumn < 0) {
            pk = ValueLong.get(row.getKey());
        } else {
            Value value = row.getValue(mainIndexColumn);
            if (value != null) {
                if (value instanceof ValueLong)
                    pk = (ValueLong) value;
                else
                    pk = ValueLong.get(value.getLong());
            } else {
                pk = ValueLong.get(row.getKey());
            }
        }
        return pk;
    }

    @Override
    public Map<List<String>, List<PageKey>> getNodeToPageKeyMap(ServerSession session, SearchRow first,
            SearchRow last) {
        ValueLong from = getPK(first);
        ValueLong to = getPK(last);
        StorageMap<Value, VersionedValue> map = getMap(session);
        return map.getNodeToPageKeyMap(from, to);
    }

    @Override
    public long getAndAddKey(long delta) {
        return dataMap.getAndAddKey(delta);
    }

    @Override
    public void setMaxKey(long maxKey) {
        dataMap.setMaxKey(ValueLong.get(maxKey));
    }

    @Override
    public boolean isAppendMode() {
        return mainIndexColumn == -1;
    }

    private final DbObjectLock dbObjectLock = new DbObjectLockImpl(DbObjectType.INDEX);
    private final ConcurrentHashMap<String, Long> replicationNameToStartKeyMap = new ConcurrentHashMap<>();

    @Override
    public boolean tryExclusiveAppendLock(ServerSession session) {
        if (replicationNameToStartKeyMap.containsKey(session.getReplicationName())) {
            return true;
        }
        return dbObjectLock.tryExclusiveLock(session);
    }

    @Override
    public void unlockAppend(ServerSession session) {
        dbObjectLock.unlock(session);
    }

    @Override
    public void setReplicationNameToStartKeyMap(Map<String, Long> replicationNameToStartKeyMap) {
        this.replicationNameToStartKeyMap.putAll(replicationNameToStartKeyMap);
    }

    @Override
    public void removeReplicationName(String replicationName) {
        if (replicationName != null)
            replicationNameToStartKeyMap.remove(replicationName);
    }

    @Override
    public boolean containsReplicationName(String replicationName) {
        return replicationNameToStartKeyMap.containsKey(replicationName);
    }

    @Override
    public long getStartKey(String replicationName) {
        Long startKey = replicationNameToStartKeyMap.get(replicationName);
        if (startKey != null)
            return startKey.longValue();
        else
            return -1;
    }

    private static class StandardPrimaryIndexCursor implements Cursor {

        private final ServerSession session;
        private final StandardTable table;
        private final StandardPrimaryIndex index;
        private final Iterator<TransactionMapEntry<Value, VersionedValue>> iterator;
        private final ValueLong last;
        private Row row;

        public StandardPrimaryIndexCursor(ServerSession session, StandardTable table, StandardPrimaryIndex index,
                Iterator<TransactionMapEntry<Value, VersionedValue>> iterator, ValueLong last) {
            this.session = session;
            this.table = table;
            this.index = index;
            this.iterator = iterator;
            this.last = last;
        }

        @Override
        public Row get() {
            return row;
        }

        @Override
        public boolean next() {
            TransactionMapEntry<Value, VersionedValue> current = iterator.hasNext() ? iterator.next() : null;
            if (last != null && current != null && current.getKey().getLong() > last.getLong()) {
                current = null;
            }
            if (current != null) {
                createRow(current);
                return true;
            } else {
                row = null;
                return false;
            }
        }

        private void createRow(TransactionMapEntry<Value, VersionedValue> current) {
            Object tv = current.getTValue();
            VersionedValue value = current.getValue();
            Value[] data = value.value.getList();
            int version = value.version;
            row = new Row(data, 0);
            row.setKey(current.getKey().getLong());
            row.setVersion(version);
            row.setTValue(tv);

            if (table.getVersion() != version) {
                ArrayList<TableAlterHistoryRecord> records = table.getDatabase().getVersionManager()
                        .getTableAlterHistoryRecord(table.getId(), version, table.getVersion());
                Value[] newValues = data;
                for (TableAlterHistoryRecord record : records) {
                    newValues = record.redo(session, newValues);
                }
                if (newValues != data) {
                    index.remove(session, row, false);
                    row = new Row(newValues, 0);
                    row.setKey(current.getKey().getLong());
                    row.setVersion(table.getVersion());
                    row.setTValue(tv);
                    index.add(session, row);
                }
            }
        }
    }
}
