/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.db.table.Table;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.BuiltInAggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;

public class OptimizableVisitor extends BooleanExpressionVisitor {

    private final Table table;

    public OptimizableVisitor(Table table) {
        this.table = table;
    }

    @Override
    public Boolean visitRownum(Rownum e) {
        return false;
    }

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return true;
    }

    @Override
    public Boolean visitJavaAggregate(JavaAggregate e) {
        // user defined aggregate functions can not be optimized
        return false;
    }

    @Override
    public Boolean visitAggregate(Aggregate e) {
        return ((BuiltInAggregate) e).isOptimizable(table) && super.visitAggregate(e);
    }
}
