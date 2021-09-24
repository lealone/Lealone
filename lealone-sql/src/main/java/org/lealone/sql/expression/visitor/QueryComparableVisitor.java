/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.function.Function;

public class QueryComparableVisitor extends BooleanExpressionVisitor {

    @Override
    public Boolean visitRownum(Rownum e) {
        return false;
    }

    @Override
    public Boolean visitSequenceValue(SequenceValue e) {
        return false;
    }

    @Override
    public Boolean visitFunction(Function e) {
        return super.visitFunction(e) && e.isDeterministic();
    }
}
