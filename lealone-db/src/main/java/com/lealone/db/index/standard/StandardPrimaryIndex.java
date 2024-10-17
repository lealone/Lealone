/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index.standard;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.RunMode;
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
import com.lealone.db.table.TableAlterHistoryRecord;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLob;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.Storage;
import com.lealone.storage.page.IPage;
import com.lealone.transaction.ITransactionalValue;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionEngine;
import com.lealone.transaction.TransactionMap;
import com.lealone.transaction.TransactionMapCursor;

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
        PrimaryKeyType keyType = new PrimaryKeyType();
        VersionedValueType vvType = new VersionedValueType(database, database.getCompareMode(),
                sortTypes, columns.length, table.getEnumColumns());

        Storage storage = database.getStorage(table.getStorageEngine());
        TransactionEngine transactionEngine = database.getTransactionEngine();
        RunMode runMode = table.getRunMode();
        Transaction t = transactionEngine.beginTransaction(runMode);
        dataMap = t.openMap(mapName, keyType, vvType, storage, table.getParameters());
        t.commit(); // 避免产生内部未提交的事务
    }

    public TransactionMap<Value, VersionedValue> getDataMap() {
        return dataMap;
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

    public boolean containsMainIndexColumn() {
        return mainIndexColumn != -1;
    }

    // 执行ddl语句增删lob字段时老记录里没有lob字段需要特殊处理
    private ValueLob getLargeObject(Row row, int columnId) {
        if (columnId >= row.getColumnCount())
            return null;
        Value v = row.getValue(columnId);
        if (v instanceof ValueLob)
            return (ValueLob) v;
        else
            return null;
    }

    private void linkLargeObject(ServerSession session, Row row, int columnId, ValueLob v) {
        ValueLob v2 = v.link(table.getDataHandler(), getId());
        if (v2.isLinked()) {
            DataHandler dh = table.getDataHandler();
            int id = dh.isTableLobStorage() ? getId() : -1;
            v2.setHandler(dh);
            session.unlinkAtRollback(v2);
            session.addDataHandler(id, dh);
            v2.setUseTableLobStorage(dh.isTableLobStorage());
        }
        if (v != v2) {
            row.setValue(columnId, v2);
        }
    }

    private void unlinkLargeObject(ServerSession session, ValueLob v) {
        if (v != null && v.isLinked()) {
            DataHandler dh;
            int id;
            if (v.isUseTableLobStorage()) {
                dh = table.getDataHandler();
                id = getId();
            } else {
                dh = database;
                id = -1;
            }
            v.setHandler(dh);
            session.unlinkAtCommit(v);
            session.addDataHandler(id, dh);
        }
    }

    public void onAddSucceeded(ServerSession session, Row row) {
        if (table.containsLargeObject()) {
            for (int columnId : table.getLargeObjectColumns()) {
                ValueLob v = getLargeObject(row, columnId);
                if (v != null)
                    linkLargeObject(session, row, columnId, v);
            }
        }
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

        AsyncCallback<Integer> ac = session.createCallback();
        TransactionMap<Value, VersionedValue> map = getMap(session);
        VersionedValue value = new VersionedValue(row.getVersion(), row.getValueList());
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
                    session.setLastIdentity(key);
                    ac.setAsyncResult(ar);
                }
            });
        } else {
            map.append(value, ar -> {
                if (ar.isSucceeded()) {
                    row.setKey(ar.getResult().getLong());
                    session.setLastIdentity(ar.getResult());
                    ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
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
            return Future.succeededFuture(
                    map.addWaitingTransaction(ValueLong.get(oldRow.getKey()), oldRow.getTValue()));

        if (table.containsLargeObject()) {
            for (int columnId : table.getLargeObjectColumns()) {
                ValueLob oldLob = getLargeObject(oldRow, columnId);
                ValueLob newLob = getLargeObject(newRow, columnId);
                // 如果lob字段不需要更新那就什么都不需要做
                if (oldLob != newLob) {
                    if (newLob != null)
                        linkLargeObject(session, newRow, columnId, newLob);
                    unlinkLargeObject(session, oldLob);
                }
            }
        }
        if (oldRow.getPage() != null)
            session.addDirtyPage(oldRow.getPage());
        VersionedValue newValue = new VersionedValue(newRow.getVersion(), newRow.getValueList());
        Value key = ValueLong.get(newRow.getKey());
        int ret = map.tryUpdate(key, newValue, updateColumns, oldRow.getTValue(), isLockedBySelf);
        session.setLastIdentity(key);
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
            for (int columnId : table.getLargeObjectColumns()) {
                ValueLob v = getLargeObject(row, columnId);
                unlinkLargeObject(session, v);
            }
        }
        if (row.getPage() != null)
            session.addDirtyPage(row.getPage());
        return Future.succeededFuture(map.tryRemove(key, tv, isLockedBySelf));
    }

    @Override
    public int tryLock(ServerSession session, Row row, int[] lockColumns) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        return map.tryLock(ValueLong.get(row.getKey()), row.getTValue(), lockColumns);
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
        return new StandardPrimaryIndexCursor(session, table, this,
                getMap(session).cursor(newParameters), to);
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
        return new StandardPrimaryIndexCursor(session, table, this, getMap(session).cursor(first), last);
    }

    @Override
    public SearchRow findFirstOrLast(ServerSession session, boolean first) {
        TransactionMap<Value, VersionedValue> map = getMap(session);
        ValueLong v = (ValueLong) (first ? map.firstKey() : map.lastKey());
        if (v == null)
            return null;
        return getRow(session, v.getLong());
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        return getRow(session, key, null);
    }

    public Row getRow(ServerSession session, long key, int[] columnIndexes) {
        Object[] objects = getMap(session).getObjects(ValueLong.get(key), columnIndexes);
        return getRow((IPage) objects[0], (ITransactionalValue) objects[1], key, objects[2]);
    }

    public Row getRow(IPage page, ITransactionalValue oldTValue, long key, Object value) {
        if (value == null) // 已经删除了
            return null;
        VersionedValue v = (VersionedValue) value;
        Row row = new Row(v.columns, 0);
        row.setKey(key);
        row.setVersion(v.version);
        row.setPage(page);
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
        getMap(session).clear();
    }

    public void repair(ServerSession session) {
        dataMap.repair();
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

    private static class StandardPrimaryIndexCursor implements Cursor {

        private final ServerSession session;
        private final StandardTable table;
        private final StandardPrimaryIndex index;
        private final TransactionMapCursor<Value, VersionedValue> tmCursor;
        private final ValueLong last;
        private Row row;

        public StandardPrimaryIndexCursor(ServerSession session, StandardTable table,
                StandardPrimaryIndex index, TransactionMapCursor<Value, VersionedValue> tmCursor,
                ValueLong last) {
            this.session = session;
            this.table = table;
            this.index = index;
            this.tmCursor = tmCursor;
            this.last = last;
        }

        @Override
        public Row get() {
            return row;
        }

        @Override
        public boolean next() {
            if (tmCursor.next()) {
                if (last != null && tmCursor.getKey().getLong() > last.getLong()) {
                    row = null;
                    return false;
                }
                createRow();
                return true;
            }
            return false;
        }

        private void createRow() {
            ITransactionalValue tv = tmCursor.getTValue();
            VersionedValue value = tmCursor.getValue();
            Value[] data = value.columns;
            int version = value.version;
            row = new Row(data, 0);
            row.setKey(tmCursor.getKey().getLong());
            row.setVersion(version);
            row.setTValue(tv);
            row.setPage(tmCursor.getPage());

            if (table.getVersion() != version) {
                ArrayList<TableAlterHistoryRecord> records = table.getDatabase().getTableAlterHistory()
                        .getRecords(table.getId(), version, table.getVersion());
                Value[] newValues = data;
                for (TableAlterHistoryRecord record : records) {
                    newValues = record.redo(session, newValues);
                }
                if (newValues != data) {
                    index.remove(session, row, false);
                    row = new Row(newValues, 0);
                    row.setKey(tmCursor.getKey().getLong());
                    row.setVersion(table.getVersion());
                    row.setTValue(tv);
                    row.setPage(tmCursor.getPage());
                    index.add(session, row);
                }
            }
        }
    }
}
