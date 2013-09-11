/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.expression;

import java.util.HashMap;

import com.codefollower.lealone.command.Parser;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.command.dml.SelectListColumnResolver;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Constant;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.index.IndexCondition;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.ColumnResolver;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueBoolean;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private Database database;
    private String schemaName;
    private String tableAlias;
    private String columnFamilyName;
    private String columnName;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;
    private boolean evaluatable;

    public ExpressionColumn(Database database, Column column) {
        this.database = database;
        this.column = column;
    }

    public ExpressionColumn(Database database, String schemaName, String tableAlias, String columnName) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    public ExpressionColumn(Database database, String schemaName, String tableAlias, String columnFamilyName, String columnName) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnFamilyName = columnFamilyName;
        this.columnName = columnName;
    }

    public String getSQL(boolean isDistributed) {
        String sql;
        boolean quote = database.getSettings().databaseToUpper;
        if (column != null) {
            sql = column.getSQL();
        } else {
            sql = quote ? Parser.quoteIdentifier(columnName) : columnName;
        }
        if (tableAlias != null) {
            String a = quote ? Parser.quoteIdentifier(tableAlias) : tableAlias;
            sql = a + "." + sql;
        }
        if (schemaName != null) {
            String s = quote ? Parser.quoteIdentifier(schemaName) : schemaName;
            sql = s + "." + sql;
        }
        return sql;
    }

    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        if (resolver instanceof TableFilter && resolver.getTableFilter().getTable().supportsColumnFamily()) {
            Table t = resolver.getTableFilter().getTable();

            //            if (!t.isStatic() && t.getRowKeyName().equalsIgnoreCase(columnName)) {
            //                if (columnFamilyName != null) {
            //                    schemaName = tableAlias;
            //                    tableAlias = columnFamilyName;
            //                    columnFamilyName = null;
            //                }
            //                if (tableAlias != null && !database.equalsIdentifiers(tableAlias, resolver.getTableAlias())) {
            //                    return;
            //                }
            //                if (schemaName != null && !database.equalsIdentifiers(schemaName, resolver.getSchemaName())) {
            //                    return;
            //                }
            //                mapColumn(resolver, t.getRowKeyColumn(), level);
            //                return;
            //            }

            if (database.equalsIdentifiers(Column.ROWKEY, columnName)) {
                Column col = t.getRowKeyColumn();
                if (col != null) {
                    mapColumn(resolver, col, level);
                    return;
                }
            }
            if (resolver.getSelect() == null) {
                Column c = t.getColumn(columnName);
                mapColumn(resolver, c, level);
                return;
            }

            String tableAlias = this.tableAlias;
            boolean useAlias = false;
            //当columnFamilyName不存在时，有可能是想使用简化的tableAlias.columnName语法
            if (columnFamilyName != null && !t.doesColumnFamilyExist(columnFamilyName)) {
                //不替换原有的tableAlias，因为有可能在另一个table中存在这样的columnFamilyName
                tableAlias = columnFamilyName;

                if (!t.doesColumnExist(columnName))
                    return;

                useAlias = true;
            }

            if (tableAlias != null && !database.equalsIdentifiers(tableAlias, resolver.getTableAlias())) {
                return;
            }
            if (schemaName != null && !database.equalsIdentifiers(schemaName, resolver.getSchemaName())) {
                return;
            }

            String fullColumnName;
            if (useAlias || columnFamilyName == null)
                fullColumnName = columnName;
            else
                fullColumnName = t.getFullColumnName(columnFamilyName, columnName);

            if (t.doesColumnExist(fullColumnName)) {
                Column c = t.getColumn(fullColumnName);
                resolver.getSelect().addColumn(resolver.getTableFilter(), c);
                mapColumn(resolver, c, level);
                return;
            }
        } else {
            if (!(resolver instanceof SelectListColumnResolver) && columnFamilyName != null) {
                schemaName = tableAlias;
                tableAlias = columnFamilyName;
                columnFamilyName = null;
            }
        }

        if (tableAlias != null && !database.equalsIdentifiers(tableAlias, resolver.getTableAlias())) {
            return;
        }
        if (schemaName != null && !database.equalsIdentifiers(schemaName, resolver.getSchemaName())) {
            return;
        }
        for (Column col : resolver.getColumns()) {
            String n = col.getName();
            if (database.equalsIdentifiers(columnName, n)) {
                mapColumn(resolver, col, level);
                return;
            }
        }
        if (database.equalsIdentifiers(Column.ROWID, columnName)) {
            Column col = resolver.getRowIdColumn();
            if (col != null) {
                mapColumn(resolver, col, level);
                return;
            }
        }
        Column[] columns = resolver.getSystemColumns();
        for (int i = 0; columns != null && i < columns.length; i++) {
            Column col = columns[i];
            if (database.equalsIdentifiers(columnName, col.getName())) {
                mapColumn(resolver, col, level);
                return;
            }
        }
    }

    private void mapColumn(ColumnResolver resolver, Column col, int level) {
        if (this.columnResolver == null) {
            queryLevel = level;
            column = col;
            this.columnResolver = resolver;
        } else if (queryLevel == level && this.columnResolver != resolver) {
            if (resolver instanceof SelectListColumnResolver) {
                // ignore - already mapped, that's ok
            } else {
                throw DbException.get(ErrorCode.AMBIGUOUS_COLUMN_NAME_1, columnName);
            }
        }
    }

    public Expression optimize(Session session) {
        if (columnResolver == null) {
            Schema schema = session.getDatabase().findSchema(tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
            if (schema != null) {
                Constant constant = schema.findConstant(columnName);
                if (constant != null) {
                    return constant.getValue();
                }
            }
            String name = columnName;
            if (tableAlias != null) {
                name = tableAlias + "." + name;
                if (schemaName != null) {
                    name = schemaName + "." + name;
                }
            }
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, name);
        }
        return columnResolver.optimize(this, column);
    }

    public void updateAggregate(Session session) {
        Value now = columnResolver.getValue(column);
        Select select = columnResolver.getSelect();
        if (select == null) {
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        HashMap<Expression, Object> values = select.getCurrentGroup();
        if (values == null) {
            // this is a different level (the enclosing query)
            return;
        }
        Value v = (Value) values.get(this);
        if (v == null) {
            values.put(this, now);
        } else {
            if (!database.areEqual(now, v)) {
                throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
            }
        }
    }

    public Value getValue(Session session) {
        Select select = columnResolver.getSelect();
        if (select != null) {
            HashMap<Expression, Object> values = select.getCurrentGroup();
            if (values != null) {
                Value v = (Value) values.get(this);
                if (v != null) {
                    return v;
                }
            }
        }
        Value value = columnResolver.getValue(column);
        if (value == null) {
            columnResolver.getValue(column);
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return value;
    }

    public int getType() {
        return column.getType();
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (columnResolver != null && tableFilter == columnResolver.getTableFilter()) {
            evaluatable = b;
        }
    }

    public Column getColumn() {
        return column;
    }

    public int getScale() {
        return column.getScale();
    }

    public long getPrecision() {
        return column.getPrecision();
    }

    public int getDisplaySize() {
        return column.getDisplaySize();
    }

    public String getOriginalColumnName() {
        return columnName;
    }

    public String getOriginalTableAliasName() {
        return tableAlias;
    }

    public String getColumnName() {
        return columnName != null ? columnName : column.getName();
    }

    public String getSchemaName() {
        Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    }

    public String getTableName() {
        Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    public String getAlias() {
        return column == null ? null : column.getName();
    }

    public boolean isAutoIncrement() {
        return column.getSequence() != null;
    }

    public int getNullable() {
        return column.isNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return false;
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return this.queryLevel < visitor.getQueryLevel();
        case ExpressionVisitor.EVALUATABLE:
            // if the current value is known (evaluatable set)
            // or if this columns belongs to a 'higher level' query and is
            // therefore just a parameter
            if (database.getSettings().nestedJoins) {
                if (visitor.getQueryLevel() < this.queryLevel) {
                    return true;
                }
                if (getTableFilter() == null) {
                    return false;
                }
                return getTableFilter().isEvaluatable();
            }
            return evaluatable || visitor.getQueryLevel() < this.queryLevel;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(column.getTable().getMaxDataModificationId());
            return true;
        case ExpressionVisitor.NOT_FROM_RESOLVER:
            return columnResolver != visitor.getResolver();
        case ExpressionVisitor.GET_DEPENDENCIES:
            if (column != null) {
                visitor.addDependency(column.getTable());
            }
            return true;
        case ExpressionVisitor.GET_COLUMNS:
            visitor.addColumn(column);
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    public int getCost() {
        return 2;
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        TableFilter tf = getTableFilter();
        if (filter == tf && column.getType() == Value.BOOLEAN) {
            IndexCondition cond = IndexCondition.get(Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(true)));
            filter.addIndexCondition(cond);
        }
    }

    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

}
