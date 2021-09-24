/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SequenceValue;

public class IndependentVisitor extends BooleanExpressionVisitor {

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return e.getQueryLevel() < getQueryLevel();
    }

    @Override
    public Boolean visitParameter(Parameter e) {
        return e.getValue() != null;
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
