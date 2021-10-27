/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.optimizer;

import org.lealone.db.session.Session;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.IExpression;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.query.Select;

/**
 * A column resolver is list of column (for example, a table) that can map a
 * column name to an actual column.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface ColumnResolver extends IExpression.Evaluator {

    /**
     * Get the schema name.
     *
     * @return the schema name
     */
    default String getSchemaName() {
        return null;
    }

    /**
     * Get the table alias.
     *
     * @return the table alias
     */
    default String getTableAlias() {
        return null;
    }

    /**
     * Get the column list.
     *
     * @return the column list
     */
    Column[] getColumns();

    /**
     * Get the list of system columns, if any.
     *
     * @return the system columns or null
     */
    default Column[] getSystemColumns() {
        return null;
    }

    /**
     * Get the row id pseudo column, if there is one.
     *
     * @return the row id column or null
     */
    default Column getRowIdColumn() {
        return null;
    }

    /**
     * Get the value for the given column.
     *
     * @param column the column
     * @return the value
     */
    Value getValue(Column column);

    /**
     * Get the table filter.
     *
     * @return the table filter
     */
    TableFilter getTableFilter();

    /**
     * Get the select statement.
     *
     * @return the select statement
     */
    Select getSelect();

    /**
     * Get the expression that represents this column.
     *
     * @param expressionColumn the expression column
     * @param column the column
     * @return the optimized expression
     */
    default Expression optimize(ExpressionColumn expressionColumn, Column column) {
        return expressionColumn;
    }

    @Override
    default IExpression optimizeExpression(Session session, IExpression e) {
        Expression expression = (Expression) e;
        expression.mapColumns(this, 0);
        return expression.optimize(session);
    }

    int getState();

    void setState(int state);

    public static final int STATE_INITIAL = 0;

    public static final int STATE_IN_AGGREGATE = 1;
}
