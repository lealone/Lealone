/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.SysProperties;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.GlobalUniqueIndex;
import org.lealone.db.index.HashIndex;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexType;
import org.lealone.db.index.NonUniqueHashIndex;
import org.lealone.db.index.StandardDelegateIndex;
import org.lealone.db.index.StandardIndex;
import org.lealone.db.index.StandardPrimaryIndex;
import org.lealone.db.index.StandardSecondaryIndex;
import org.lealone.db.result.Row;
import org.lealone.db.result.SortOrder;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardTable extends Table {

    private final ConcurrentHashMap<ServerSession, ServerSession> sharedSessions = new ConcurrentHashMap<>();
    private final StandardPrimaryIndex primaryIndex;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private final StorageEngine storageEngine;
    private final Map<String, String> parameters;
    private final boolean globalTemporary;

    private long lastModificationId;
    private int changesSinceAnalyze;
    private int nextAnalyze;
    private boolean containsLargeObject;
    private Column rowIdColumn;
    private boolean containsGlobalUniqueIndex;
    private long rowCount;

    ArrayList<TableAlterHistoryRecord> tableAlterHistoryRecords;

    public StandardTable(CreateTableData data, StorageEngine storageEngine) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.storageEngine = storageEngine;
        if (data.storageEngineParams != null) {
            parameters = data.storageEngineParams;
        } else {
            parameters = new CaseInsensitiveMap<>();
        }
        globalTemporary = data.globalTemporary;

        String initReplicationEndpoints = null;
        String replicationName = data.session.getReplicationName();
        if (replicationName != null) {
            int pos = replicationName.indexOf('@');
            if (pos != -1) {
                initReplicationEndpoints = replicationName.substring(0, pos);
                parameters.put("initReplicationEndpoints", initReplicationEndpoints);
            }
        }
        parameters.put("isShardingMode", data.session.getDatabase().isShardingMode() + "");

        isHidden = data.isHidden;
        nextAnalyze = database.getSettings().analyzeAuto;

        setTemporary(data.temporary);
        setColumns(data.columns.toArray(new Column[0]));
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
            }
        }
        primaryIndex = new StandardPrimaryIndex(data.session, this);
        indexes.add(primaryIndex);
        tableAlterHistoryRecords = getDatabase().getTableAlterHistoryRecord(id, 0, getVersion() - 1);
    }

    public String getMapName() {
        return primaryIndex.getMapName();
    }

    public StorageEngine getStorageEngine() {
        return storageEngine;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Set the row count of this table.
     *
     * @param count the row count
     */
    public void setRowCount(long count) {
        this.rowCount = count;
    }

    @Override
    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (isTemporary()) {
            if (isGlobalTemporary()) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        } else if (isPersistIndexes()) {
            buff.append("CACHED ");
        } else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        if (isHidden) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        buff.append("\nPROCESSING MODE ").append(getProcessingMode().name());
        String storageEngineName = storageEngine.getName();
        if (storageEngineName != null) {
            String d = getDatabase().getSettings().defaultStorageEngine;
            if (d == null || !storageEngineName.endsWith(d)) {
                buff.append("\nENGINE \"");
                buff.append(storageEngineName);
                buff.append('\"');
            }
        }
        if (parameters != null && !parameters.isEmpty()) {
            buff.append(" PARAMETERS");
            Database.appendMap(buff, parameters);
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        if (isHidden) {
            buff.append("\nHIDDEN");
        }
        return buff.toString();
    }

    @Override
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL() + " CASCADE";
    }

    @Override
    public void close(ServerSession session) {
        for (Index index : indexes) {
            index.close(session);
        }
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

    public void checkRowCount(ServerSession session, Index index, int offset) {
        if (SysProperties.CHECK && !database.isMultiVersion()) {
            long rc = index.getRowCount(session);
            if (rc != rowCount + offset) {
                DbException.throwInternalError("rowCount expected " + (rowCount + offset) //
                        + " got " + rc + " " + getName() + "." + index.getName());
            }
        }
    }

    /**
     * Create a row from the values.
     *
     * @param data the value list
     * @return the row
     */
    public static Row createRow(Value[] data) {
        return new Row(data, Row.MEMORY_CALCULATE);
    }

    @Override
    public boolean lock(ServerSession session, boolean exclusive, boolean forceLockEvenInMvcc) {
        if ((exclusive || forceLockEvenInMvcc) && !sharedSessions.containsKey(session)) {
            sharedSessions.put(session, session);
            session.addLock(this);
        }
        return true;
    }

    @Override
    public void unlock(ServerSession s) {
        if (sharedSessions.containsKey(s)) {
            sharedSessions.remove(s);
        }
    }

    public static String getDeadlockDetails(ArrayList<ServerSession> sessions) {
        // We add the thread details here to make it easier for customers to
        // match up these error messages with their own logs.
        StringBuilder buff = new StringBuilder();
        for (ServerSession s : sessions) {
            Table lock = s.getWaitForLock();
            Thread thread = s.getWaitForLockThread();
            buff.append("\nSession ").append(s.toString()).append(" on thread ").append(thread.getName())
                    .append(" is waiting to lock ").append(lock.toString()).append(" while locking ");
            int i = 0;
            for (Table t : s.getLocks()) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(t.toString());
            }
            buff.append('.');
        }
        return buff.toString();
    }

    @Override
    public ArrayList<ServerSession> checkDeadlock(ServerSession session, ServerSession clash,
            Set<ServerSession> visited) {
        // only one deadlock check at any given time
        synchronized (StandardTable.class) {
            if (clash == null) {
                // verification is started
                clash = session;
                visited = new HashSet<>();
            } else if (clash == session) {
                // we found a circle where this session is involved
                return new ArrayList<>(0);
            } else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a circle, but the sessions in the circle need to
                // find it out themselves
                return null;
            }
            visited.add(session);
            ArrayList<ServerSession> error = null;
            for (ServerSession s : sharedSessions.keySet()) {
                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                Table t = s.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(s, clash, visited);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }
            return error;
        }
    }

    @Override
    public boolean isLockedExclusively() {
        return false; // lockExclusiveSession != null;
    }

    @Override
    public boolean isLockedExclusivelyBy(ServerSession session) {
        return false; // lockExclusiveSession == session;
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (int i = 0, size = constraints.size(); i < size; i++) {
                    Constraint c = constraints.get(i);
                    if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public Row getRow(ServerSession session, long key) {
        return primaryIndex.getRow(session, key);
    }

    public Row getRow(ServerSession session, long key, int[] columnIndexes) {
        return primaryIndex.getRow(session, key, columnIndexes);
    }

    @Override
    public Index addIndex(ServerSession session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        int mainIndexColumn = getMainIndexColumn(indexType, cols);
        if (indexType.isDelegate()) {
            index = createDelegateIndex(indexId, indexName, indexType, mainIndexColumn);
        } else {
            if (database.isStarting()) {
                if (database.getStorage(storageEngine).hasMap(getMapNameForIndex(indexId))) {
                    mainIndexColumn = -1;
                }
            } else if (primaryIndex.getRowCountMax() != 0) {
                mainIndexColumn = -1;
            }
            if (mainIndexColumn != -1) {
                index = createDelegateIndex(indexId, indexName, indexType, mainIndexColumn);
            } else if (isGlobalUniqueIndex(session, indexType)) {
                index = new GlobalUniqueIndex(session, this, indexId, indexName, cols, indexType);
                containsGlobalUniqueIndex = true;
            } else if (indexType.isHash() && cols.length <= 1) { // TODO 是否要支持多版本
                if (indexType.isUnique()) {
                    index = new HashIndex(this, indexId, indexName, cols, indexType);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName, cols, indexType);
                }
            } else {
                index = new StandardSecondaryIndex(session, this, indexId, indexName, cols, indexType);
            }
            if (index instanceof StandardIndex && index.needRebuild()) {
                rebuildIndex(session, (StandardIndex) index, indexName);
            }
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    private boolean isGlobalUniqueIndex(ServerSession session, IndexType indexType) {
        return indexType.isUnique() && !indexType.isPrimaryKey() && session.isShardingMode()
                && session.getConnectionInfo() != null && !session.getConnectionInfo().isEmbedded(); // &&
                                                                                                     // !session.isLocal();
    }

    private StandardDelegateIndex createDelegateIndex(int indexId, String indexName, IndexType indexType,
            int mainIndexColumn) {
        primaryIndex.setMainIndexColumn(mainIndexColumn);
        return new StandardDelegateIndex(this, indexId, indexName, primaryIndex, indexType);
    }

    private void rebuildIndex(ServerSession session, StandardIndex index, String indexName) {
        try {
            if (index.isInMemory()) {
                // in-memory
                rebuildIndexBuffered(session, index);
            } else {
                rebuildIndexBlockMerge(session, index);
            }
        } catch (DbException e) {
            getSchema().freeUniqueName(indexName);
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                trace.error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        }
    }

    private void rebuildIndexBlockMerge(ServerSession session, StandardIndex index) {
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        int bufferSize = database.getMaxMemoryRows() / 2;
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        ArrayList<String> bufferNames = Utils.newSmallArrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                sortRows(buffer, index);
                String mapName = database.getStorage(storageEngine).nextTemporaryMapName();
                index.addRowsToBuffer(session, buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        sortRows(buffer, index);
        if (bufferNames.size() > 0) {
            String mapName = database.getStorage(storageEngine).nextTemporaryMapName();
            index.addRowsToBuffer(session, buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            index.addBufferedRows(session, bufferNames);
        } else {
            addRowsToIndex(session, buffer, index);
        }
        if (SysProperties.CHECK && remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining + " " + getName());
        }
    }

    private void rebuildIndexBuffered(ServerSession session, Index index) {
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        int bufferSize = (int) Math.min(total, database.getMaxMemoryRows());
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                addRowsToIndex(session, buffer, index);
            }
            remaining--;
        }
        addRowsToIndex(session, buffer, index);
        if (SysProperties.CHECK && remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining + " " + getName());
        }
    }

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (primaryIndex.getMainIndexColumn() != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return -1;
        }
        IndexColumn first = cols[0];
        if (first.sortType != SortOrder.ASCENDING) {
            return -1;
        }
        switch (first.column.getType()) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
            break;
        default:
            return -1;
        }
        return first.column.getColumnId();
    }

    private static void addRowsToIndex(ServerSession session, ArrayList<Row> list, Index index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    private static void sortRows(ArrayList<Row> list, final Index index) {
        Collections.sort(list, new Comparator<Row>() {
            @Override
            public int compare(Row r1, Row r2) {
                return index.compareRows(r1, r2);
            }
        });
    }

    @Override
    public void addRow(ServerSession session, Row row) {
        tryAddRow(session, row, null);
    }

    @Override
    public boolean tryAddRow(ServerSession session, Row row, Transaction.Listener globalListener) {
        row.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        try {
            // 第一个是PrimaryIndex
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (globalListener == null) // 如果是null就用同步api
                    index.add(session, row);
                else
                    index.tryAdd(session, row, globalListener);
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return false;
    }

    @Override
    public void updateRow(ServerSession session, Row oldRow, Row newRow, List<Column> updateColumns) {
        tryUpdateRow(session, oldRow, newRow, updateColumns, null);
    }

    @Override
    public boolean tryUpdateRow(ServerSession session, Row oldRow, Row newRow, List<Column> updateColumns,
            Transaction.Listener globalListener) {
        newRow.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        try {
            // 第一个是PrimaryIndex
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (globalListener == null) {
                    index.update(session, oldRow, newRow, updateColumns);
                } else {
                    if (!index.tryUpdate(session, oldRow, newRow, updateColumns, globalListener))
                        return false;
                }
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return true;
    }

    @Override
    public void removeRow(ServerSession session, Row row) {
        tryRemoveRow(session, row, false);
    }

    @Override
    public boolean tryRemoveRow(ServerSession session, Row row) {
        return tryRemoveRow(session, row, true);
    }

    private boolean tryRemoveRow(ServerSession session, Row row, boolean async) {
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        try {
            for (int i = indexes.size() - 1; i >= 0; i--) {
                Index index = indexes.get(i);
                if (async) {
                    if (!index.tryRemove(session, row))
                        return false;
                } else {
                    index.remove(session, row);
                }
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return false;
    }

    @Override
    public boolean tryLockRow(ServerSession session, Row row) {
        // 只锁主索引即可
        return primaryIndex.tryLock(session, row);
    }

    @Override
    public void truncate(ServerSession session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        changesSinceAnalyze = 0;
    }

    protected void analyzeIfRequired(ServerSession session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        // TODO
        // int rows = session.getDatabase().getSettings().analyzeSample / 10;
        // Analyze.analyzeTable(session, this, rows, false);
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public TableType getTableType() {
        return TableType.STANDARD_TABLE;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return primaryIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return primaryIndex.getRowCountApproximation();
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
            database.lockMeta(session);
        }
        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will
        // call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
            // needed for session temporary indexes
            indexes.remove(index);
        }
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObjectType.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError("index not dropped: " + index.getName());
                }
            }
        }
        primaryIndex.remove(session);
        database.removeMeta(session, getId());
        close(session);
        invalidate();
    }

    @Override
    public long getDiskSpaceUsed() {
        return primaryIndex.getDiskSpaceUsed();
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, -1);
        }
        return rowIdColumn;
    }

    @Override
    public String toString() {
        return getSQL();
    }

    /**
     * Mark the transaction as committed, so that the modification counter of
     * the database is incremented.
     */
    public void commit() {
        if (database != null) {
            lastModificationId = database.getNextModificationDataId();
        }
    }

    @Override
    public boolean containsGlobalUniqueIndex() {
        return containsGlobalUniqueIndex;
    }

    // 只要组合数据库id和表或索引的id就能得到一个全局唯一的map名了
    public String getMapNameForTable(int id) {
        return getMapName("t", database.getId(), id);
    }

    public String getMapNameForIndex(int id) {
        return getMapName("i", database.getId(), id);
    }

    private static String getMapName(Object... args) {
        StringBuilder name = new StringBuilder();
        for (Object arg : args) {
            if (name.length() > 0)
                name.append(Constants.NAME_SEPARATOR);
            name.append(arg.toString());
        }
        return name.toString();
    }

    @Override
    public List<StorageMap<? extends Object, ? extends Object>> getAllStorageMaps() {
        List<StorageMap<? extends Object, ? extends Object>> maps = new ArrayList<>(indexes.size());
        for (Index i : indexes) {
            if (i.getStorageMap() != null)
                maps.add(i.getStorageMap());
        }
        return maps;
    }

    @Override
    public void setNewColumns(Column[] columns) {
        this.oldColumns = this.columns;
        setColumns(columns);
    }

    private Column[] oldColumns;

    @Override
    public Column[] getOldColumns() {
        return oldColumns;
    }

    @Override
    public void incrementVersion() {
        super.incrementVersion();
        tableAlterHistoryRecords = getDatabase().getTableAlterHistoryRecord(id, 0, getVersion() - 1);
    }
}
