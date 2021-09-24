/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.Variable;
import org.lealone.sql.expression.aggregate.JavaAggregate;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.query.Select;

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
