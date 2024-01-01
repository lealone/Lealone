/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.Rownum;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.Variable;
import com.lealone.sql.expression.aggregate.JavaAggregate;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.query.Select;

public class DeterministicVisitor extends BooleanExpressionVisitor {

    @Override
    public Boolean visitRownum(Rownum e) {
        return false;
    }

    @Override
    public Boolean visitSequenceValue(SequenceValue e) {
        return false;
    }

    @Override
    public Boolean visitVariable(Variable e) {
        return false;
    }

    @Override
    public Boolean visitJavaAggregate(JavaAggregate e) {
        // TODO optimization: some functions are deterministic, but we don't
        // know (no setting for that)
        return false;
    }

    @Override
    public Boolean visitFunction(Function e) {
        return super.visitFunction(e) && e.isDeterministic();
    }

    @Override
    public Boolean visitSelect(Select s) {
        return s.isDeterministic() && super.visitSelect(s);
    }

    @Override
    protected ExpressionVisitorBase<Boolean> copy() {
        return new DeterministicVisitor();
    }
}
