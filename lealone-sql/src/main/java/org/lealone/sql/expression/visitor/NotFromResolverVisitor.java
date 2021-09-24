/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.optimizer.ColumnResolver;

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
