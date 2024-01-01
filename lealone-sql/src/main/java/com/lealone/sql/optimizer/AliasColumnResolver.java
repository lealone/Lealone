/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.optimizer;

import com.lealone.db.session.Session;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.sql.IExpression;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.query.Select;

// 处理在where和having中出现别名的情况，如:
// SELECT id AS A FROM test where A>=0
// SELECT id/3 AS A, COUNT(*) FROM test GROUP BY A HAVING A>=0
public class AliasColumnResolver extends ColumnResolverBase {

    private final Select select;
    private final Expression expression;
    private final Column column;

    public AliasColumnResolver(Select select, Expression expression, Column column) {
        this.select = select;
        this.expression = expression;
        this.column = column;
    }

    @Override
    public Value getValue(Column col) {
        return expression.getValue(select.getSession());
    }

    @Override
    public Column[] getColumns() {
        return new Column[] { column };
    }

    @Override
    public TableFilter getTableFilter() {
        return select.getTableFilter();
    }

    @Override
    public Select getSelect() {
        return select;
    }

    @Override
    public Expression optimize(ExpressionColumn expressionColumn, Column col) {
        return expression.optimize(select.getSession());
    }

    @Override
    public Value getExpressionValue(Session session, IExpression e, Object data) {
        return expression.getValue(select.getSession());
    }
}
