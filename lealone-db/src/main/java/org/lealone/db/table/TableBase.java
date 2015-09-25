/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.util.ArrayList;

import org.lealone.common.message.DbException;
import org.lealone.common.util.New;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.value.CompareMode;
import org.lealone.common.value.DataType;
import org.lealone.common.value.Value;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;

/**
 * The base class of a regular table, or a user defined table.
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin
 */
public abstract class TableBase extends Table {
    protected final boolean globalTemporary;
    protected boolean containsLargeObject;
    protected long rowCount;

    protected Index scanIndex;
    protected final ArrayList<Index> indexes = New.arrayList();

    protected long lastModificationId;
    private int changesSinceAnalyze;
    private int nextAnalyze;
    private Column rowIdColumn;

    public TableBase(CreateTableData data) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.storageEngine = data.storageEngine;
        this.globalTemporary = data.globalTemporary;

        setTemporary(data.temporary);
        initColumns(data.columns);
        nextAnalyze = database.getSettings().analyzeAuto;
        this.isHidden = data.isHidden;
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
            }
        }

    }

    protected void initColumns(ArrayList<Column> columns) {
        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        setColumns(cols);
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
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
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL() + " CASCADE";
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
        if (storageEngine != null) {
            String d = getDatabase().getSettings().defaultStorageEngine;
            if (d == null || !storageEngine.endsWith(d)) {
                buff.append("\nENGINE \"");
                buff.append(storageEngine);
                buff.append('\"');
            }
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
    public void close(Session session) {
        for (Index index : indexes) {
            index.close(session);
        }
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

    /**
     * Read the given row.
     *
     * @param session the session
     * @param key unique key
     * @return the row
     */
    public Row getRow(Session session, long key) {
        return scanIndex.getRow(session, key);
    }

    @Override
    public void addRow(Session session, Row row) {
    }

    public void checkRowCount(Session session, Index index, int offset) {
        if (SysProperties.CHECK && !database.isMultiVersion()) {
            long rc = index.getRowCount(session);
            if (rc != rowCount + offset) {
                DbException.throwInternalError("rowCount expected " + (rowCount + offset) //
                        + " got " + rc + " " + getName() + "." + index.getName());
            }
        }
    }

    @Override
    public Index getScanIndex(Session session) {
        return indexes.get(0);
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public abstract Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols,
            IndexType indexType, boolean create, String indexComment);

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            return getScanIndex(session).getRowCount(session);
        }
        return rowCount;
    }

    @Override
    public void removeRow(Session session, Row row) {
    }

    @Override
    public void truncate(Session session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        rowCount = 0;
        changesSinceAnalyze = 0;
    }

    protected void analyzeIfRequired(Session session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        // int rows = session.getDatabase().getSettings().analyzeSample;
        // Analyze.analyzeTable(session, this, rows, false); //TODO
    }

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param exclusive true for write locks, false for read locks
     * @param forceLockEvenInMvcc lock even in the MVCC mode
     * @return true if the table was already exclusively locked by this session.
     * @throws DbException if a lock timeout occurred
     */
    @Override
    public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        return false;
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
    public String toString() {
        return getSQL();
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public String getTableType() {
        return Table.TABLE;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    @Override
    public long getRowCountApproximation() {
        return scanIndex.getRowCountApproximation();
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    @Override
    public long getDiskSpaceUsed() {
        return scanIndex.getDiskSpaceUsed();
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, -1);
        }
        return rowIdColumn;
    }

}
