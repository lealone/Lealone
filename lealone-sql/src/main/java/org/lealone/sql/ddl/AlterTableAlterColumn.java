/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;
import java.util.HashSet;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.New;
import org.lealone.db.Database;
import org.lealone.db.DbObject;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.expression.ExpressionVisitor;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaStatement {

    private Table table;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    private Expression defaultExpression;
    private Expression newSelectivity;
    private String addBefore;
    private String addAfter;
    private boolean ifNotExists;
    private ArrayList<Column> columnsToAdd;

    public AlterTableAlterColumn(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return type;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setOldColumn(Column oldColumn) {
        this.oldColumn = oldColumn;
    }

    public void setAddBefore(String before) {
        this.addBefore = before;
    }

    public void setAddAfter(String after) {
        this.addAfter = after;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setSelectivity(Expression selectivity) {
        newSelectivity = selectivity;
    }

    public void setDefaultExpression(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public void setNewColumn(Column newColumn) {
        this.newColumn = newColumn;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setNewColumns(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.lock(session, true, true);
        if (newColumn != null) {
            checkDefaultReferencesTable((Expression) newColumn.getDefaultExpression());
        }
        if (columnsToAdd != null) {
            for (Column column : columnsToAdd) {
                checkDefaultReferencesTable((Expression) column.getDefaultExpression());
            }
        }
        switch (type) {
        case SQLStatement.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
            if (!oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNoNullValues();
            oldColumn.setNullable(false);
            db.updateMeta(session, table);
            break;
        }
        case SQLStatement.ALTER_TABLE_ALTER_COLUMN_NULL: {
            if (oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNullable();
            oldColumn.setNullable(true);
            db.updateMeta(session, table);
            break;
        }
        case SQLStatement.ALTER_TABLE_ALTER_COLUMN_DEFAULT: {
            Sequence sequence = oldColumn.getSequence();
            checkDefaultReferencesTable(defaultExpression);
            oldColumn.setSequence(null);
            oldColumn.setDefaultExpression(session, defaultExpression);
            removeSequence(sequence);
            db.updateMeta(session, table);
            break;
        }
        case SQLStatement.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
            // if the change is only increasing the precision, then we don't
            // need to copy the table because the length is only a constraint,
            // and does not affect the storage structure.
            if (oldColumn.isWideningConversion(newColumn)) {
                convertAutoIncrementColumn(newColumn);
                oldColumn.copy(newColumn);
                db.updateMeta(session, table);
            } else {
                oldColumn.setSequence(null);
                oldColumn.setDefaultExpression(session, null);
                oldColumn.setConvertNullToDefault(false);
                if (oldColumn.isNullable() && !newColumn.isNullable()) {
                    checkNoNullValues();
                } else if (!oldColumn.isNullable() && newColumn.isNullable()) {
                    checkNullable();
                }
                convertAutoIncrementColumn(newColumn);
                addTableAlterHistoryRecords();
            }
            break;
        }
        case SQLStatement.ALTER_TABLE_ADD_COLUMN: {
            // ifNotExists only supported for single column add
            if (ifNotExists && columnsToAdd.size() == 1 && table.doesColumnExist(columnsToAdd.get(0).getName())) {
                break;
            }
            for (Column column : columnsToAdd) {
                if (column.isAutoIncrement()) {
                    int objId = getObjectId();
                    column.convertAutoIncrementToSequence(session, getSchema(), objId, table.isTemporary());
                }
            }
            addTableAlterHistoryRecords();
            break;
        }
        case SQLStatement.ALTER_TABLE_DROP_COLUMN: {
            if (table.getColumns().length == 1) {
                throw DbException.get(ErrorCode.CANNOT_DROP_LAST_COLUMN, oldColumn.getSQL());
            }
            table.dropSingleColumnConstraintsAndIndexes(session, oldColumn);
            addTableAlterHistoryRecords();
            break;
        }
        case SQLStatement.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
            int value = newSelectivity.optimize(session).getValue(session).getInt();
            oldColumn.setSelectivity(value);
            db.updateMeta(session, table);
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    private void checkDefaultReferencesTable(Expression defaultExpression) {
        if (defaultExpression == null) {
            return;
        }
        HashSet<DbObject> dependencies = New.hashSet();
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        defaultExpression.isEverything(visitor);
        if (dependencies.contains(table)) {
            throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, defaultExpression.getSQL());
        }
    }

    private void convertAutoIncrementColumn(Column c) {
        if (c.isAutoIncrement()) {
            if (c.isPrimaryKey()) {
                c.setOriginalSQL("IDENTITY");
            } else {
                int objId = getObjectId();
                c.convertAutoIncrementToSequence(session, getSchema(), objId, table.isTemporary());
            }
        }
    }

    private void removeSequence(Sequence sequence) {
        if (sequence != null) {
            table.removeSequence(sequence);
            if (sequence.getBelongsToTable()) {
                sequence.setBelongsToTable(false);
                Database db = session.getDatabase();
                db.removeSchemaObject(session, sequence);
            }
        }
    }

    private void checkNullable() {
        for (Index index : table.getIndexes()) {
            if (index.getColumnIndex(oldColumn) < 0) {
                continue;
            }
            IndexType indexType = index.getIndexType();
            if (indexType.isPrimaryKey() || indexType.isHash()) {
                throw DbException.get(ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL());
            }
        }
    }

    private void checkNoNullValues() {
        String sql = "SELECT COUNT(*) FROM " + table.getSQL() + " WHERE " + oldColumn.getSQL() + " IS NULL";
        StatementBase command = (StatementBase) session.prepareStatement(sql);
        Result result = command.query(0);
        result.next();
        if (result.currentRow()[0].getInt() > 0) {
            throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, oldColumn.getSQL());
        }
    }

    private void addTableAlterHistoryRecords() {
        if (table.isTemporary()) {
            throw DbException.getUnsupportedException("TEMP TABLE");
        }
        addTableAlterHistoryRecords0();
        try {
            // check if a view would become invalid
            // (because the column to drop is referenced or so)
            checkViewsAreValid(table);
        } catch (DbException e) {
            table.setNewColumns(table.getOldColumns());
            throw DbException.get(ErrorCode.VIEW_IS_INVALID_2, e, getSQL(), e.getMessage());
        }

        try {
            table.incrementVersion();
        } catch (DbException e) {
            table.setNewColumns(table.getOldColumns());
            throw e;
        }

        // 通知元数据改变了，原有的结果集缓存要废弃了
        table.setModified();
    }

    private void addTableAlterHistoryRecords0() {
        Database db = session.getDatabase();
        Column[] columns = table.getColumns();
        ArrayList<Column> newColumns = New.arrayList();
        for (Column col : columns) {
            newColumns.add(col.getClone());
        }
        if (type == SQLStatement.ALTER_TABLE_DROP_COLUMN) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
            db.addTableAlterHistoryRecord(table.getId(), table.getVersion(), type, Integer.toString(position));
        } else if (type == SQLStatement.ALTER_TABLE_ADD_COLUMN) {
            int position;
            if (addBefore != null) {
                position = table.getColumn(addBefore).getColumnId();
            } else if (addAfter != null) {
                position = table.getColumn(addAfter).getColumnId() + 1;
            } else {
                position = columns.length;
            }
            StringBuilder buff = new StringBuilder();
            buff.append(position);
            for (Column column : columnsToAdd) {
                buff.append(',').append(column.getCreateSQL());
                newColumns.add(position++, column);
            }
            db.addTableAlterHistoryRecord(table.getId(), table.getVersion(), type, buff.toString());
        } else if (type == SQLStatement.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
            newColumns.add(position, newColumn);
            db.addTableAlterHistoryRecord(table.getId(), table.getVersion(), type,
                    position + "," + newColumn.getCreateSQL());
        }

        table.setNewColumns(newColumns.toArray(new Column[0]));
    }

    /**
     * Check that a table or view is still valid.
     *
     * @param tableOrView the table or view to check
     */
    private void checkViewsAreValid(DbObject tableOrView) {
        for (DbObject view : tableOrView.getChildren()) {
            if (view instanceof TableView) {
                String sql = ((TableView) view).getQuery();
                // check if the query is still valid
                // do not execute, not even with limit 1, because that could
                // have side effects or take a very long time
                session.prepareStatement(sql);
                checkViewsAreValid(view);
            }
        }
    }

}
