/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.query.Select;

public class EvaluatableVisitor extends BooleanExpressionVisitor {

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return e.isEvaluatable(getQueryLevel());
    }

    @Override
    public Boolean visitSelect(Select s) {
        return s.isEvaluatable() && super.visitSelect(s);
    }

    @Override
    protected ExpressionVisitorBase<Boolean> copy() {
        return new EvaluatableVisitor();
    }
}
