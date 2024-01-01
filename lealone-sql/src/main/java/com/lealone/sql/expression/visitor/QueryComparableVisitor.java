/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.Rownum;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.function.Function;

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
