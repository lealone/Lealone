/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.Calculator;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;

public class CalculateVisitor extends ExpressionVisitorBase<Void> {

    private Calculator calculator;

    public CalculateVisitor(Calculator calculator) {
        this.calculator = calculator;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        e.calculate(calculator);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        e.calculate(calculator);
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        e.calculate(calculator);
        return null;
    }
}
