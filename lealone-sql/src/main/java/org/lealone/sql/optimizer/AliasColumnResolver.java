/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.optimizer;

import org.lealone.db.session.Session;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.IExpression;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.query.Select;

// 处理在where和having中出现别名的情况，如:
// SELECT id AS A FROM test where A>=0
// SELECT id/3 AS A, COUNT(*) FROM test GROUP BY A HAVING A>=0
public class AliasColumnResolver implements ColumnResolver {

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
