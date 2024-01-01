/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.Rownum;
import com.lealone.sql.expression.SequenceValue;

public class IndependentVisitor extends BooleanExpressionVisitor {

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return e.getQueryLevel() < getQueryLevel();
    }

    @Override
    public Boolean visitParameter(Parameter e) {
        return e.isValueSet();
    }

    @Override
    public Boolean visitRownum(Rownum e) {
        return false;
    }

    @Override
    public Boolean visitSequenceValue(SequenceValue e) {
        return false;
    }
}
