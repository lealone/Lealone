/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectType;
import org.lealone.db.ProcessingMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SimpleRow;
import org.lealone.db.result.SimpleRowValue;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObjectBase;
import org.lealone.db.schema.Sequence;
import org.lealone.db.schema.TriggerObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.IExpression;
import org.lealone.transaction.Transaction;

/**
 * This is the base class for most tables.
 * A table contains a list of columns and a list of rows.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class Table extends SchemaObjectBase {

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_CACHED = 0;

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_MEMORY = 1;

    /**
     * The columns of this table.
     */
    protected Column[] columns;

    /**
     * Protected tables are not listed in the meta data and are excluded when
     * using the SCRIPT command.
     */
    protected boolean isHidden;

    /**
     * The compare mode used for this table.
     */
    private final CompareMode compareMode;
    private final HashMap<String, Column> columnMap;
    private final boolean persistIndexes;
    private final boolean persistData;

    private ArrayList<TriggerObject> triggers;
    private ArrayList<Constraint> constraints;
    private ArrayList<Sequence> sequences;
    private ArrayList<TableView> views;

    private boolean checkForeignKeyConstraints = true;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Row nullRow;

    private int version = -1;
    private String packageName;
    private String codePath;
    private ProcessingMode processingMode = ProcessingMode.OLTP;

    public Table(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
        super(schema, id, name);
        compareMode = database.getCompareMode();
        columnMap = database.newStringMap();
        this.persistIndexes = persistIndexes;
        this.persistData = persistData;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.TABLE_OR_VIEW;
    }

    public int getVersion() {
        if (version == -1) {
            synchronized (this) {
                version = getDatabase().getVersionManager().getVersion(getId());
            }
        }
        return version;
    }

    public void incrementVersion() {
        synchronized (this) {
            version++;
            getDatabase().getVersionManager().updateVersion(getId(), version);
        }
    }

    public void decrementVersion() {
        if (version == -1)
            return;
        synchronized (this) {
            version--;
            getDatabase().getVersionManager().updateVersion(getId(), version);
        }
    }

    @Override
    public void rename(String newName) {
        super.rename(newName);
        if (constraints != null) {
            for (int i = 0, size = constraints.size(); i < size; i++) {
                Constraint constraint = constraints.get(i);
                constraint.rebuild();
            }
        }
    }

    /**
     * Close the table object and flush changes.
     *
     * @param session the session
     */
    public void close(ServerSession session) {
        // nothing to do
    }

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param exclusive true for write locks, false for read locks
     * @throws DbException if a lock timeout occurred
     */
    public boolean lock(ServerSession session, boolean exclusive) {
        // nothing to do
        return false;
    }

    public boolean trySharedLock(ServerSession session) {
        // nothing to do
        return false;
    }

    public boolean tryExclusiveLock(ServerSession session) {
        // nothing to do
        return false;
    }

    /**
     * Release the lock for this session.
     *
     * @param s the session
     */
    public void unlock(ServerSession s) {
        // nothing to do
    }

    public void unlock(ServerSession s, boolean succeeded) {
        unlock(s);
    }

    /**
     * Check if this table is locked exclusively.
     *
     * @return true if it is.
     */
    public boolean isLockedExclusively() {
        return false;
    }

    /**
     * Check if the table is exclusively locked by this session.
     *
     * @param session the session
     * @return true if it is
     */
    public boolean isLockedExclusivelyBy(ServerSession session) {
        return false;
    }

    /**
     * Create an index for this table
     *
     * @param session the session
     * @param indexName the name of the index
     * @param indexId the id
     * @param cols the index columns
     * @param indexType the index type
     * @param create whether this is a new index
     * @param indexComment the comment
     * @return the index
     */
    public Index addIndex(ServerSession session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment, LockTable lockTable) {
        throw newUnsupportedException();
    }

    private DbException newUnsupportedException() {
        return DbException.getUnsupportedException(getTableType().toString());
    }

    /**
     * Add a row to the table and all indexes.
     *
     * @param session the session
     * @param row the row
     * @throws DbException if a constraint was violated
     */
    public void addRow(ServerSession session, Row row) {
        throw newUnsupportedException();
    }

    public boolean tryAddRow(ServerSession session, Row row, Transaction.Listener globalListener) {
        throw newUnsupportedException();
    }

    /**
     * Update a row from the table and all indexes.
     *
     * @param session the session
     * @param oldRow the old row
     * @param newRow the new row
     */
    public void updateRow(ServerSession session, Row oldRow, Row newRow, List<Column> updateColumns) {
        throw newUnsupportedException();
    }

    public int tryUpdateRow(ServerSession session, Row oldRow, Row newRow, List<Column> updateColumns,
            Transaction.Listener globalListener) {
        throw newUnsupportedException();
    }

    /**
     * Remove a row from the table and all indexes.
     *
     * @param session the session
     * @param row the row
     */
    public void removeRow(ServerSession session, Row row) {
        throw newUnsupportedException();
    }

    public int tryRemoveRow(ServerSession session, Row row, Transaction.Listener globalListener) {
        throw newUnsupportedException();
    }

    public boolean tryLockRow(ServerSession session, Row row) {
        throw newUnsupportedException();
    }

    /**
     * Remove all rows from the table and indexes.
     *
     * @param session the session
     */
    public void truncate(ServerSession session) {
        throw newUnsupportedException();
    }

    /**
     * Check if this table supports ALTER TABLE.
     *
     * @throws DbException if it is not supported
     */
    public void checkSupportAlter() {
        throw newUnsupportedException();
    }

    @Override
    public void checkRename() {
        throw newUnsupportedException();
    }

    /**
     * Get the table type
     *
     * @return the table type
     */
    public abstract TableType getTableType();

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(ServerSession session);

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ArrayList<Index> getIndexes();

    /**
     * Get the last data modification id.
     *
     * @return the modification id
     */
    public abstract long getMaxDataModificationId();

    /**
     * Check if the table is deterministic.
     *
     * @return true if it is
     */
    public abstract boolean isDeterministic();

    /**
     * Check if this table can be referenced.
     *
     * @return true if it can
     */
    public boolean canReference() {
        return true;
    }

    /**
     * Check if this table can be dropped.
     *
     * @return true if it can
     */
    public abstract boolean canDrop();

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @return true if it can
     */
    public abstract boolean canGetRowCount();

    /**
     * Get the row count for this table.
     *
     * @param session the session
     * @return the row count
     */
    public abstract long getRowCount(ServerSession session);

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation();

    public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * Get the row id column if this table has one.
     *
     * @return the row id column, or null
     */
    public Column getRowIdColumn() {
        return null;
    }

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public void addDependencies(Set<DbObject> dependencies) {
        if (dependencies.contains(this)) {
            // avoid endless recursion
            return;
        }
        if (sequences != null) {
            for (Sequence s : sequences) {
                dependencies.add(s);
            }
        }
        if (columns != null)
            for (Column col : columns) {
                col.getDependencies(dependencies);
            }
        if (constraints != null) {
            for (Constraint c : constraints) {
                c.getDependencies(dependencies);
            }
        }
        dependencies.add(this);
    }

    @Override
    public List<DbObject> getChildren() {
        ArrayList<DbObject> children = Utils.newSmallArrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            children.addAll(indexes);
        }
        if (constraints != null) {
            children.addAll(constraints);
        }
        if (triggers != null) {
            children.addAll(triggers);
        }
        if (sequences != null) {
            children.addAll(sequences);
        }
        if (views != null) {
            children.addAll(views);
        }
        ArrayList<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedObject() == this) {
                children.add(right);
            }
        }
        return children;
    }

    protected void setColumns(Column[] columns) {
        this.columns = columns;
        if (!columnMap.isEmpty()) {
            columnMap.clear();
        }
        for (int i = 0, len = columns.length; i < len; i++) {
            Column col = columns[i];
            if (col.getType() == Value.UNKNOWN) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, col.getSQL());
            }
            String columnName = col.getName();
            if (columnMap.containsKey(columnName)) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, columnName);
            }
            columnMap.put(columnName, col);
            col.setTable(this, i);
        }
    }

    public void setNewColumns(Column[] columns) {
    }

    public Column[] getOldColumns() {
        return columns;
    }

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public void renameColumn(Column column, String newName) {
        for (Column c : columns) {
            if (c == column) {
                continue;
            }
            if (c.getName().equals(newName)) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, newName);
            }
        }
        columnMap.remove(column.getName());
        column.rename(newName);
        columnMap.put(newName, column);
    }

    public ArrayList<TableView> getViews() {
        return views;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, LockTable lockTable) {
        while (views != null && views.size() > 0) {
            TableView view = views.get(0);
            views.remove(0);
            schema.remove(session, view, lockTable);
        }
        while (triggers != null && triggers.size() > 0) {
            TriggerObject trigger = triggers.get(0);
            triggers.remove(0);
            schema.remove(session, trigger, lockTable);
        }
        while (constraints != null && constraints.size() > 0) {
            Constraint constraint = constraints.get(0);
            constraints.remove(0);
            schema.remove(session, constraint, lockTable);
        }
        for (Right right : database.getAllRights()) {
            if (right.getGrantedObject() == this) {
                database.removeDatabaseObject(session, right, lockTable);
            }
        }
        // must delete sequences later (in case there is a power failure
        // before removing the table object)
        while (sequences != null && sequences.size() > 0) {
            Sequence sequence = sequences.get(0);
            sequences.remove(0);
            if (!isTemporary()) {
                // only remove if no other table depends on this sequence
                // this is possible when calling ALTER TABLE ALTER COLUMN
                if (database.getDependentTable(sequence, this) == null) {
                    schema.remove(session, sequence, lockTable);
                }
            }
        }
    }

    /**
     * Check that this column is not referenced by a multi-column constraint or
     * multi-column index. If it is, an exception is thrown. Single-column
     * references and indexes are dropped.
     *
     * @param session the session
     * @param col the column
     * @throws DbException if the column is referenced by multi-column
     *             constraints or indexes
     */
    public void dropSingleColumnConstraintsAndIndexes(ServerSession session, Column col) {
        ArrayList<Constraint> constraintsToDrop = Utils.newSmallArrayList();
        if (constraints != null) {
            for (int i = 0, size = constraints.size(); i < size; i++) {
                Constraint constraint = constraints.get(i);
                HashSet<Column> columns = constraint.getReferencedColumns(this);
                if (!columns.contains(col)) {
                    continue;
                }
                if (columns.size() == 1) {
                    constraintsToDrop.add(constraint);
                } else {
                    throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, constraint.getSQL());
                }
            }
        }
        ArrayList<Index> indexesToDrop = Utils.newSmallArrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index.getCreateSQL() == null) {
                    continue;
                }
                if (index.getColumnIndex(col) < 0) {
                    continue;
                }
                if (index.getColumns().length == 1) {
                    indexesToDrop.add(index);
                } else {
                    throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, index.getSQL());
                }
            }
        }
        for (Constraint c : constraintsToDrop) {
            c.getSchema().remove(session, c);
        }
        for (Index i : indexesToDrop) {
            // the index may already have been dropped when dropping the constraint
            if (getIndexes().contains(i)) {
                i.getSchema().remove(session, i);
            }
        }
    }

    public Row getTemplateRow() {
        return new Row(new Value[columns.length], Row.MEMORY_CALCULATE);
    }

    /**
     * Get a new simple row object.
     *
     * @param singleColumn if only one value need to be stored
     * @return the simple row object
     */
    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn) {
            return new SimpleRowValue(columns.length);
        }
        return new SimpleRow(new Value[columns.length]);
    }

    public synchronized Row getNullRow() {
        if (nullRow == null) {
            nullRow = new Row(new Value[columns.length], 1);
            for (int i = 0; i < columns.length; i++) {
                nullRow.setValue(i, ValueNull.INSTANCE);
            }
        }
        return nullRow;
    }

    public Column[] getColumns() {
        return columns;
    }

    /**
     * Get the column at the given index.
     *
     * @param index the column index (0, 1,...)
     * @return the column
     */
    public Column getColumn(int index) {
        return columns[index];
    }

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @return the column
     * @throws DbException if the column was not found
     */
    public Column getColumn(String columnName) {
        Column column = columnMap.get(columnName);
        if (column == null) {
            if (database.equalsIdentifiers(Column.ROWID, columnName)) {
                return getRowIdColumn();
            }
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Does the column with the given name exist?
     *
     * @param columnName the column name
     * @return true if the column exists
     */
    public boolean doesColumnExist(String columnName) {
        return columnMap.containsKey(columnName);
    }

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public Index findPrimaryKey() {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index idx = indexes.get(i);
                if (idx.getIndexType().isPrimaryKey()) {
                    return idx;
                }
            }
        }
        return null;
    }

    public Index getPrimaryKey() {
        Index index = findPrimaryKey();
        if (index != null) {
            return index;
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, Constants.PREFIX_PRIMARY_KEY);
    }

    /**
     * Validate all values in this row, convert the values if required, and
     * update the sequence values if required. This call will also set the
     * default values if required and set the computed column if there are any.
     *
     * @param session the session
     * @param row the row
     */
    public void validateConvertUpdateSequence(ServerSession session, Row row) {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            Value v2;
            if (column.getComputed()) {
                // force updating the value
                value = null;
                v2 = column.computeValue(session, row);
            }
            v2 = column.validateConvertUpdateSequence(session, value);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
    }

    private static void remove(ArrayList<? extends DbObject> list, DbObject obj) {
        if (list != null) {
            int i = list.indexOf(obj);
            if (i >= 0) {
                list.remove(i);
            }
        }
    }

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public void removeIndex(Index index) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            remove(indexes, index);
            if (index.getIndexType().isPrimaryKey()) {
                for (Column col : index.getColumns()) {
                    col.setPrimaryKey(false);
                }
            }
        }
    }

    /**
     * Remove the given view from the list.
     *
     * @param view the view to remove
     */
    public void removeView(TableView view) {
        remove(views, view);
    }

    /**
     * Remove the given constraint from the list.
     *
     * @param constraint the constraint to remove
     */
    public void removeConstraint(Constraint constraint) {
        remove(constraints, constraint);
    }

    /**
     * Remove a sequence from the table. Sequences are used as identity columns.
     *
     * @param session the session
     * @param sequence the sequence to remove
     */
    public void removeSequence(Sequence sequence) {
        remove(sequences, sequence);
    }

    /**
     * Remove the given trigger from the list.
     *
     * @param trigger the trigger to remove
     */
    public void removeTrigger(TriggerObject trigger) {
        remove(triggers, trigger);
    }

    /**
     * Add a view to this table.
     *
     * @param view the view to add
     */
    public void addView(TableView view) {
        views = add(views, view);
    }

    /**
     * Add a constraint to the table.
     *
     * @param constraint the constraint to add
     */
    public void addConstraint(Constraint constraint) {
        if (constraints == null || constraints.indexOf(constraint) < 0) {
            constraints = add(constraints, constraint);
        }
    }

    public ArrayList<Constraint> getConstraints() {
        return constraints;
    }

    /**
     * Add a sequence to this table.
     *
     * @param sequence the sequence to add
     */
    public void addSequence(Sequence sequence) {
        sequences = add(sequences, sequence);
    }

    /**
     * Add a trigger to this table.
     *
     * @param trigger the trigger to add
     */
    public void addTrigger(TriggerObject trigger) {
        triggers = add(triggers, trigger);
    }

    private static <T> ArrayList<T> add(ArrayList<T> list, T obj) {
        if (list == null) {
            list = new ArrayList<>(1);
        }
        // self constraints are two entries in the list
        list.add(obj);
        return list;
    }

    /**
     * Fire the triggers for this table.
     *
     * @param session the session
     * @param type the trigger type
     * @param beforeAction whether 'before' triggers should be called
     */
    public void fire(ServerSession session, int type, boolean beforeAction) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                trigger.fire(session, type, beforeAction);
            }
        }
    }

    /**
     * Check whether this table has a select trigger.
     *
     * @return true if it has
     */
    public boolean hasSelectTrigger() {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                if (trigger.isSelectTrigger()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if row based triggers or constraints are defined.
     * In this case the fire after and before row methods need to be called.
     *
     *  @return if there are any triggers or rows defined
     */
    public boolean fireRow() {
        return (constraints != null && !constraints.isEmpty()) || (triggers != null && !triggers.isEmpty());
    }

    /**
     * Fire all triggers that need to be called before a row is updated.
     *
     * @param session the session
     * @param oldRow the old data or null for an insert
     * @param newRow the new data or null for a delete
     * @return true if no further action is required (for 'instead of' triggers)
     */
    public boolean fireBeforeRow(ServerSession session, Row oldRow, Row newRow) {
        boolean done = fireRow(session, oldRow, newRow, true, false);
        fireConstraints(session, oldRow, newRow, true);
        return done;
    }

    private void fireConstraints(ServerSession session, Row oldRow, Row newRow, boolean before) {
        if (constraints != null) {
            // don't use enhanced for loop to avoid creating objects
            for (int i = 0, size = constraints.size(); i < size; i++) {
                Constraint constraint = constraints.get(i);
                if (constraint.isBefore() == before) {
                    constraint.checkRow(session, this, oldRow, newRow);
                }
            }
        }
    }

    /**
     * Fire all triggers that need to be called after a row is updated.
     *
     *  @param session the session
     *  @param oldRow the old data or null for an insert
     *  @param newRow the new data or null for a delete
     *  @param rollback when the operation occurred within a rollback
     */
    public void fireAfterRow(ServerSession session, Row oldRow, Row newRow, boolean rollback) {
        fireRow(session, oldRow, newRow, false, rollback);
        if (!rollback) {
            fireConstraints(session, oldRow, newRow, false);
        }
    }

    private boolean fireRow(ServerSession session, Row oldRow, Row newRow, boolean beforeAction, boolean rollback) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                boolean done = trigger.fireRow(session, oldRow, newRow, beforeAction, rollback);
                if (done) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isGlobalTemporary() {
        return false;
    }

    /**
     * Check if this table can be truncated.
     *
     * @return true if it can
     */
    public boolean canTruncate() {
        return false;
    }

    /**
     * Enable or disable foreign key constraint checking for this table.
     *
     * @param session the session
     * @param enabled true if checking should be enabled
     * @param checkExisting true if existing rows must be checked during this call
     */
    public void setCheckForeignKeyConstraints(ServerSession session, boolean enabled, boolean checkExisting) {
        if (enabled && checkExisting) {
            if (constraints != null) {
                for (Constraint c : constraints) {
                    c.checkExistingData(session);
                }
            }
        }
        checkForeignKeyConstraints = enabled;
    }

    public boolean getCheckForeignKeyConstraints() {
        return checkForeignKeyConstraints;
    }

    /**
     * Get the index that has the given column as the first element.
     * This method returns null if no matching index is found.
     *
     * @param first if the min value should be returned
     * @return the index or null
     */
    public Index getIndexForColumn(Column column) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index.canGetFirstOrLast()) {
                    int idx = index.getColumnIndex(column);
                    if (idx == 0) {
                        return index;
                    }
                }
            }
        }
        return null;
    }

    public boolean getOnCommitDrop() {
        return onCommitDrop;
    }

    public void setOnCommitDrop(boolean onCommitDrop) {
        this.onCommitDrop = onCommitDrop;
    }

    public boolean getOnCommitTruncate() {
        return onCommitTruncate;
    }

    public void setOnCommitTruncate(boolean onCommitTruncate) {
        this.onCommitTruncate = onCommitTruncate;
    }

    /**
     * If the index is still required by a constraint, transfer the ownership to
     * it. Otherwise, the index is removed.
     *
     * @param session the session
     * @param index the index that is no longer required
     */
    public void removeIndexOrTransferOwnership(ServerSession session, Index index, LockTable lockTable) {
        boolean stillNeeded = false;
        if (constraints != null) {
            for (Constraint cons : constraints) {
                if (cons.usesIndex(index)) {
                    cons.setIndexOwner(index);
                    database.updateMeta(session, cons);
                    stillNeeded = true;
                }
            }
        }
        if (!stillNeeded) {
            schema.remove(session, index, lockTable);
        }
    }

    public boolean isPersistIndexes() {
        return persistIndexes;
    }

    public boolean isPersistData() {
        return persistData;
    }

    /**
     * Compare two values with the current comparison mode. The values may be of
     * different type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSafe(Value a, Value b) {
        if (a == b) {
            return 0;
        }
        int dataType = Value.getHigherOrder(a.getType(), b.getType());
        a = a.convertTo(dataType);
        b = b.convertTo(dataType);
        return a.compareTypeSafe(b, compareMode);
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    /**
     * Tests if the table can be written. Usually, this depends on the
     * database.checkWritingAllowed method, but some tables (eg. TableLink)
     * overwrite this default behaviour.
     */
    public void checkWritingAllowed() {
        database.checkWritingAllowed();
    }

    /**
     * Get or generate a default value for the given column.
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    public Value getDefaultValue(ServerSession session, Column column) {
        IExpression defaultExpr = column.getDefaultExpression();
        Value v;
        if (defaultExpr == null) {
            v = column.validateConvertUpdateSequence(session, null);
        } else {
            v = defaultExpr.getValue(session);
        }
        return column.convert(v);
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        this.isHidden = hidden;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getCodePath() {
        return codePath;
    }

    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    public void setProcessingMode(ProcessingMode mode) {
        processingMode = mode;
    }

    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    public boolean containsLargeObject() {
        return false;
    }

    public Row getRow(ServerSession session, long key) {
        return null;
    }

    public Row getRow(ServerSession session, long key, Object oldTransactionalValue) {
        return null;
    }
}
