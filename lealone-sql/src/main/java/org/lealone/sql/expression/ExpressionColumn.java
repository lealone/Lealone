/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.expression.ExpressionVisitor;
import org.lealone.db.index.IndexCondition;
import org.lealone.db.schema.Constant;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Column;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.sql.Parser;
import org.lealone.sql.dml.Select;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression implements org.lealone.db.expression.ExpressionColumn {

    private Database database;
    private String databaseName;
    private String schemaName;
    private String tableAlias;
    private String columnName;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;
    private boolean evaluatable;

    private Select select;

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

    public ExpressionColumn(String databaseName, String schemaName, String tableAlias, String columnName) {
        this.database = null;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    private Database getDatabase() {
        if (database == null) {
            database = LealoneDatabase.getInstance().getDatabase(databaseName);
        }
        return database;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        String sql;
        boolean quote = getDatabase().getSettings().databaseToUpper;
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

    @Override
    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        getDatabase();
        if (select == null)
            select = (Select) resolver.getSelect();

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
            throw DbException.get(ErrorCode.AMBIGUOUS_COLUMN_NAME_1, columnName);
        }
    }

    @Override
    public Expression optimize(ServerSession session) {
        getDatabase();
        if (columnResolver == null) {
            Schema schema = session.getDatabase()
                    .findSchema(tableAlias == null ? session.getCurrentSchemaName() : schemaName);
            if (schema != null) {
                Constant constant = schema.findConstant(columnName);
                if (constant != null) {
                    return ValueExpression.get(constant.getValue());
                }
            }

            // 处理在where和having中出现别名的情况，如:
            // SELECT id AS A FROM mytable where A>=0
            // SELECT id/3 AS A, COUNT(*) FROM mytable GROUP BY A HAVING A>=0
            if (select != null) {
                for (Expression e : select.getExpressions()) {
                    if (database.equalsIdentifiers(columnName, e.getAlias()))
                        return e.getNonAliasExpression().optimize(session);
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
        return (Expression) columnResolver.optimize(this, column);
    }

    @Override
    public void updateAggregate(ServerSession session) {
        Value now = columnResolver.getValue(column);
        Select select = (Select) columnResolver.getSelect();
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
        }
    }

    @Override
    public Value getValue(ServerSession session) {
        Select select = (Select) columnResolver.getSelect();
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

    @Override
    public int getType() {
        return column.getType();
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (columnResolver != null && tableFilter == columnResolver.getTableFilter()) {
            evaluatable = b;
        }
    }

    @Override
    public Column getColumn() {
        return column;
    }

    @Override
    public int getScale() {
        return column.getScale();
    }

    @Override
    public long getPrecision() {
        return column.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return column.getDisplaySize();
    }

    public String getOriginalColumnName() {
        return columnName;
    }

    public String getOriginalTableAliasName() {
        return tableAlias;
    }

    @Override
    public String getColumnName() {
        return columnName != null ? columnName : column.getName();
    }

    @Override
    public String getSchemaName() {
        Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    }

    @Override
    public String getTableName() {
        Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    @Override
    public String getAlias() {
        if (column != null) {
            return column.getName();
        }
        if (tableAlias != null) {
            return tableAlias + "." + columnName;
        }
        return columnName;
    }

    @Override
    public boolean isAutoIncrement() {
        return column.getSequence() != null;
    }

    @Override
    public int getNullable() {
        return column.isNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return false;
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return this.queryLevel < visitor.getQueryLevel();
        case ExpressionVisitor.EVALUATABLE:
            // if the current value is known (evaluatable set)
            // or if this columns belongs to a 'higher level' query and is
            // therefore just a parameter
            if (getDatabase().getSettings().nestedJoins) {
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

    @Override
    public int getCost() {
        return 2;
    }

    @Override
    public void createIndexConditions(ServerSession session, TableFilter filter) {
        TableFilter tf = getTableFilter();
        if (filter == tf && column.getType() == Value.BOOLEAN) {
            IndexCondition cond = IndexCondition.get(Comparison.EQUAL, this,
                    ValueExpression.get(ValueBoolean.get(true)));
            filter.addIndexCondition(cond);
        }
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

}
