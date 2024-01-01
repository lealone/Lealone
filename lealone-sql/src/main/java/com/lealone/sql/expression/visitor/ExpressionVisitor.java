/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.Alias;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.ExpressionList;
import com.lealone.sql.expression.Operation;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.Rownum;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.Variable;
import com.lealone.sql.expression.Wildcard;
import com.lealone.sql.expression.aggregate.AGroupConcat;
import com.lealone.sql.expression.aggregate.Aggregate;
import com.lealone.sql.expression.aggregate.JavaAggregate;
import com.lealone.sql.expression.condition.CompareLike;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.condition.ConditionAndOr;
import com.lealone.sql.expression.condition.ConditionExists;
import com.lealone.sql.expression.condition.ConditionIn;
import com.lealone.sql.expression.condition.ConditionInConstantSet;
import com.lealone.sql.expression.condition.ConditionInSelect;
import com.lealone.sql.expression.condition.ConditionNot;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.expression.function.JavaFunction;
import com.lealone.sql.expression.function.TableFunction;
import com.lealone.sql.expression.subquery.SubQuery;
import com.lealone.sql.query.Select;
import com.lealone.sql.query.SelectUnion;

public interface ExpressionVisitor<R> {

    public default ExpressionVisitor<R> incrementQueryLevel(int offset) {
        return this;
    }

    public default int getQueryLevel() {
        return 0;
    }

    R visitExpression(Expression e);

    R visitAlias(Alias e);

    R visitExpressionColumn(ExpressionColumn e);

    R visitExpressionList(ExpressionList e);

    R visitOperation(Operation e);

    R visitParameter(Parameter e);

    R visitRownum(Rownum e);

    R visitSequenceValue(SequenceValue e);

    R visitSubQuery(SubQuery e);

    R visitValueExpression(ValueExpression e);

    R visitVariable(Variable e);

    R visitWildcard(Wildcard e);

    R visitCompareLike(CompareLike e);

    R visitComparison(Comparison e);

    R visitConditionAndOr(ConditionAndOr e);

    R visitConditionExists(ConditionExists e);

    R visitConditionIn(ConditionIn e);

    R visitConditionInConstantSet(ConditionInConstantSet e);

    R visitConditionInSelect(ConditionInSelect e);

    R visitConditionNot(ConditionNot e);

    R visitAggregate(Aggregate e);

    R visitAGroupConcat(AGroupConcat e);

    R visitJavaAggregate(JavaAggregate e);

    R visitFunction(Function e);

    R visitJavaFunction(JavaFunction e);

    R visitTableFunction(TableFunction e);

    R visitSelect(Select s);

    R visitSelectUnion(SelectUnion su);
}
