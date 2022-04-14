/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.RunMode;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexRebuilder;
import org.lealone.db.index.IndexType;
import org.lealone.db.index.hash.NonUniqueHashIndex;
import org.lealone.db.index.hash.UniqueHashIndex;
import org.lealone.db.index.standard.StandardDelegateIndex;
import org.lealone.db.index.standard.StandardPrimaryIndex;
import org.lealone.db.index.standard.StandardSecondaryIndex;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.result.Row;
import org.lealone.db.result.SortOrder;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.Transaction;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardTable extends Table {

    private final StandardPrimaryIndex primaryIndex;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private final ArrayList<Index> indexesExcludeDelegate = Utils.newSmallArrayList();
    private final StorageEngine storageEngine;
    private final Map<String, String> parameters;
    private final boolean globalTemporary;

    private long lastModificationId;
    private int changesSinceAnalyze;
    private int nextAnalyze;
    private boolean containsLargeObject;
    private Column rowIdColumn;

    public StandardTable(CreateTableData data, StorageEngine storageEngine) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.storageEngine = storageEngine;
        if (data.storageEngineParams != null) {
            parameters = data.storageEngineParams;
        } else {
            parameters = new CaseInsensitiveMap<>();
        }
        globalTemporary = data.globalTemporary;

        String initReplicationNodes = null;
        String replicationName = data.session.getReplicationName();
        if (replicationName != null) {
            int pos = replicationName.indexOf('@');
            if (pos != -1) {
                initReplicationNodes = replicationName.substring(0, pos);
                parameters.put("initReplicationNodes", initReplicationNodes);
            }
        }
        if (data.isMemoryTable())
            parameters.put("inMemory", "1");
        parameters.put("isShardingMode", data.session.isShardingMode() + "");
        RunMode runMode = data.session.getRunMode();
        if (runMode == RunMode.REPLICATION || runMode == RunMode.SHARDING)
            parameters.put("isDistributed", "true");

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
        indexesExcludeDelegate.add(primaryIndex);
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
        if (getPackageName() != null) {
            buff.append("\nPACKAGE '").append(getPackageName()).append("'");
        }
        if (getCodePath() != null) {
            buff.append("\nGENERATE CODE '").append(getCodePath()).append("'");
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

    @Override
    public Row getRow(ServerSession session, long key) {
        return primaryIndex.getRow(session, key);
    }

    public Row getRow(ServerSession session, long key, int[] columnIndexes) {
        return primaryIndex.getRow(session, key, columnIndexes);
    }

    @Override
    public Row getRow(ServerSession session, long key, Object oldTransactionalValue) {
        return primaryIndex.getRow(session, key, oldTransactionalValue);
    }

    @Override
    public Index addIndex(ServerSession session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment, DbObjectLock lock) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
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
            } else if (indexType.isHash() && cols.length <= 1) {
                if (indexType.isUnique()) {
                    index = new UniqueHashIndex(this, indexId, indexName, indexType, cols);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName, indexType, cols);
                }
            } else {
                index = new StandardSecondaryIndex(session, this, indexId, indexName, indexType, cols);
            }
            if (index.needRebuild()) {
                new IndexRebuilder(session, storageEngine, this, index).rebuild();
            }
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                schema.add(session, index, lock);
            }
        }
        indexes.add(index);
        if (!(index instanceof StandardDelegateIndex))
            indexesExcludeDelegate.add(index);
        setModified();
        return index;
    }

    private StandardDelegateIndex createDelegateIndex(int indexId, String indexName, IndexType indexType,
            int mainIndexColumn) {
        primaryIndex.setMainIndexColumn(mainIndexColumn);
        return new StandardDelegateIndex(primaryIndex, this, indexId, indexName, indexType);
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

    @Override
    public Future<Integer> addRow(ServerSession session, Row row) {
        row.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        try {
            // 第一个是PrimaryIndex
            for (int i = 0; i < size; i++) {
                Index index = indexesExcludeDelegate.get(i);
                index.add(session, row).onComplete(ar -> {
                    if (count.decrementAndGet() == 0) {
                        ac.setAsyncResult(ar);
                    }
                });
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return ac;
    }

    @Override
    public Future<Integer> updateRow(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf) {
        newRow.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        try {
            // 第一个是PrimaryIndex
            for (int i = 0; i < size; i++) {
                Index index = indexesExcludeDelegate.get(i);
                index.update(session, oldRow, newRow, updateColumns, isLockedBySelf).onComplete(ar -> {
                    if (count.decrementAndGet() == 0) {
                        ac.setAsyncResult(ar);
                    }
                });
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return ac;
    }

    @Override
    public Future<Integer> removeRow(ServerSession session, Row row, boolean isLockedBySelf) {
        lastModificationId = database.getNextModificationDataId();
        Transaction t = session.getTransaction();
        int savepointId = t.getSavepointId();
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        try {
            for (int i = size - 1; i >= 0; i--) {
                Index index = indexesExcludeDelegate.get(i);
                index.remove(session, row, isLockedBySelf).onComplete(ar -> {
                    if (count.decrementAndGet() == 0) {
                        ac.setAsyncResult(ar);
                    }
                });
            }
        } catch (Throwable e) {
            t.rollbackToSavepoint(savepointId);
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
        return ac;
    }

    @Override
    public boolean tryLockRow(ServerSession session, Row row, int[] lockColumns, boolean isForUpdate) {
        // 只锁主索引即可
        return primaryIndex.tryLock(session, row, lockColumns, isForUpdate);
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

    @Override
    public boolean containsLargeObject() {
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
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
        }
        super.removeChildrenAndResources(session, lock);
        // go backwards because database.removeIndex will
        // call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                schema.remove(session, index, lock);
            }
            // needed for session temporary indexes
            indexes.remove(index);
            indexesExcludeDelegate.remove(index);
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
        close(session);
    }

    @Override
    public void removeIndex(Index index) {
        super.removeIndex(index);
        indexesExcludeDelegate.remove(index);
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
    public long getAndAddKey(long delta) {
        return getScanIndex(null).getAndAddKey(delta);
    }
}
