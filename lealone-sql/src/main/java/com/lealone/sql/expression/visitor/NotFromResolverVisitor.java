/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.optimizer.ColumnResolver;

public class NotFromResolverVisitor extends BooleanExpressionVisitor {

    private final ColumnResolver resolver;

    public NotFromResolverVisitor(ColumnResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return e.getColumnResolver() != resolver;
    }
}
