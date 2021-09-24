/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Constant;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.sql.Parser;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.visitor.ExpressionVisitor;
import org.lealone.sql.optimizer.AliasColumnResolver;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.IndexCondition;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.ValueVector;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private Database database;
    private String databaseName;
    private String schemaName;
    private String tableAlias;
    private String columnName;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;

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

    public ColumnResolver getColumnResolver() {
        return columnResolver;
    }

    public int getQueryLevel() {
        return queryLevel;
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

    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        getDatabase();
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

        // 处理在where和having中出现别名的情况
        Select select = resolver.getSelect();
        if (select != null) {
            for (Expression e : select.getExpressions()) {
                // 只有Alias才需要处理
                if ((e instanceof Alias) && (database.equalsIdentifiers(columnName, e.getAlias()))) {
                    Column col = new Column(columnName, Value.NULL);
                    resolver = new AliasColumnResolver(select, e.getNonAliasExpression(), col);
                    mapColumn(resolver, col, level);
                    return;
                }
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
            Schema schema = session.getDatabase().findSchema(session,
                    tableAlias == null ? session.getCurrentSchemaName() : schemaName);
            if (schema != null) {
                Constant constant = schema.findConstant(session, columnName);
                if (constant != null) {
                    return ValueExpression.get(constant.getValue());
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

    @Override
    public void updateAggregate(ServerSession session) {
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
        if (v == null) { // 只取第一条
            Value now = columnResolver.getValue(column);
            values.put(this, now);
        }
    }

    @Override
    public void updateVectorizedAggregate(ServerSession session, ValueVector bvv) {
        Select select = columnResolver.getSelect();
        if (select == null) {
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        HashMap<Expression, Object> values = select.getCurrentGroup();
        if (values == null) {
            // this is a different level (the enclosing query)
            return;
        }
        ValueVector v = (ValueVector) values.get(this);
        if (v == null) { // 只取第一条
            ValueVector now = columnResolver.getValueVector(column);
            values.put(this, now);
        }
    }

    @Override
    public Value getValue(ServerSession session) {
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
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return value;
    }

    @Override
    public ValueVector getValueVector(ServerSession session, ValueVector bvv) {
        ValueVector vv = columnResolver.getValueVector(column);
        if (vv == null) {
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return vv.filter(bvv);
    }

    @Override
    public int getType() {
        return column.getType();
    }

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

    public boolean isEvaluatable(int queryLevel) {
        // if this column belongs to a 'higher level' query and is
        // therefore just a parameter
        if (queryLevel < this.queryLevel) {
            return true;
        }
        if (getTableFilter() == null) {
            return false;
        }
        return getTableFilter().isEvaluatable();
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

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitExpressionColumn(this);
    }
}
