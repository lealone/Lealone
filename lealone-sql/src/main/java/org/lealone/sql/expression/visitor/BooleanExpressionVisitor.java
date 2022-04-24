/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import java.util.ArrayList;

import org.lealone.sql.expression.Alias;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionList;
import org.lealone.sql.expression.Operation;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Variable;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;
import org.lealone.sql.expression.condition.CompareLike;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionExists;
import org.lealone.sql.expression.condition.ConditionIn;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionInSelect;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.function.JavaFunction;
import org.lealone.sql.expression.function.TableFunction;
import org.lealone.sql.expression.subquery.SubQuery;
import org.lealone.sql.query.Query;
import org.lealone.sql.query.Select;
import org.lealone.sql.query.SelectUnion;

public abstract class BooleanExpressionVisitor extends ExpressionVisitorBase<Boolean> {

    @Override
    public Boolean visitExpression(Expression e) {
        return true;
    }

    @Override
    public Boolean visitAlias(Alias e) {
        return e.getNonAliasExpression().accept(this);
    }

    @Override
    public Boolean visitExpressionColumn(ExpressionColumn e) {
        return true;
    }

    @Override
    public Boolean visitExpressionList(ExpressionList e) {
        for (Expression e2 : e.getList()) {
            if (!e2.accept(this))
                return false;
        }
        return true;
    }

    @Override
    public Boolean visitOperation(Operation e) {
        return e.getLeft().accept(this) && (e.getRight() == null || e.getRight().accept(this));
    }

    @Override
    public Boolean visitParameter(Parameter e) {
        return true;
    }

    @Override
    public Boolean visitRownum(Rownum e) {
        return true;
    }

    @Override
    public Boolean visitSequenceValue(SequenceValue e) {
        return true;
    }

    @Override
    public Boolean visitSubQuery(SubQuery e) {
        return visitQuery(e.getQuery());
    }

    protected Boolean visitQuery(Query query) {
        return query.accept(this);
    }

    @Override
    public Boolean visitValueExpression(ValueExpression e) {
        return true;
    }

    @Override
    public Boolean visitVariable(Variable e) {
        return true;
    }

    @Override
    public Boolean visitWildcard(Wildcard e) {
        return true;
    }

    @Override
    public Boolean visitCompareLike(CompareLike e) {
        return e.getLeft().accept(this) && e.getRight().accept(this)
                && (e.getEscape() == null || e.getEscape().accept(this));
    }

    @Override
    public Boolean visitComparison(Comparison e) {
        return e.getLeft().accept(this) && (e.getRight() == null || e.getRight().accept(this));
    }

    @Override
    public Boolean visitConditionAndOr(ConditionAndOr e) {
        return e.getLeft().accept(this) && e.getRight().accept(this);
    }

    @Override
    public Boolean visitConditionExists(ConditionExists e) {
        return visitQuery(e.getQuery());
    }

    @Override
    public Boolean visitConditionIn(ConditionIn e) {
        if (!e.getLeft().accept(this)) {
            return false;
        }
        for (Expression e2 : e.getValueList()) {
            if (!e2.accept(this)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitConditionInConstantSet(ConditionInConstantSet e) {
        return e.getLeft().accept(this);
    }

    @Override
    public Boolean visitConditionInSelect(ConditionInSelect e) {
        if (!e.getLeft().accept(this)) {
            return false;
        }
        return visitQuery(e.getQuery());
    }

    @Override
    public Boolean visitConditionNot(ConditionNot e) {
        return e.getCondition().accept(this);
    }

    @Override
    public Boolean visitAggregate(Aggregate e) {
        return e.getOn() == null || e.getOn().accept(this);
    }

    @Override
    public Boolean visitAGroupConcat(AGroupConcat e) {
        if (!visitAggregate(e)) {
            return false;
        }
        if (e.getGroupConcatSeparator() != null && !e.getGroupConcatSeparator().accept(this)) {
            return false;
        }
        if (e.getGroupConcatOrderList() != null) {
            for (int i = 0, size = e.getGroupConcatOrderList().size(); i < size; i++) {
                SelectOrderBy o = e.getGroupConcatOrderList().get(i);
                if (!o.expression.accept(this)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Boolean visitJavaAggregate(JavaAggregate e) {
        for (Expression e2 : e.getArgs()) {
            if (e != null && !e2.accept(this)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitFunction(Function e) {
        for (Expression e2 : e.getArgs()) {
            if (e2 != null && !e2.accept(this)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitJavaFunction(JavaFunction e) {
        return visitFunction(e);
    }

    @Override
    public Boolean visitTableFunction(TableFunction e) {
        return visitFunction(e);
    }

    @Override
    public Boolean visitSelect(Select s) {
        ExpressionVisitor<Boolean> v2 = incrementQueryLevel(1);
        ArrayList<Expression> expressions = s.getExpressions();
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i);
            if (!e.accept(v2))
                return false;
        }
        if (s.getCondition() != null) {
            if (!s.getCondition().accept(v2))
                return false;
        }
        if (s.getHaving() != null) {
            if (!s.getHaving().accept(v2))
                return false;
        }
        return true;
    }

    @Override
    public Boolean visitSelectUnion(SelectUnion su) {
        return su.getLeft().accept(this) && su.getRight().accept(this);
    }
}
