/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;
import org.lealone.sql.expression.condition.ConditionExists;
import org.lealone.sql.expression.condition.ConditionIn;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionInSelect;
import org.lealone.sql.expression.subquery.SubQuery;
import org.lealone.sql.optimizer.ColumnResolver;

public class MapColumnsVisitor extends VoidExpressionVisitor {

    private ColumnResolver resolver;
    private int level;

    public MapColumnsVisitor(ColumnResolver resolver, int level) {
        this.resolver = resolver;
        this.level = level;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        e.mapColumns(resolver, level);
        return null;
    }

    @Override
    public Void visitSubQuery(SubQuery e) {
        level++;
        super.visitSubQuery(e);
        level--;
        return null;
    }

    @Override
    public Void visitConditionExists(ConditionExists e) {
        level++;
        super.visitConditionExists(e);
        level--;
        return null;
    }

    @Override
    public Void visitConditionIn(ConditionIn e) {
        int level = this.level;
        super.visitConditionIn(e);
        e.setQueryLevel(level);
        return null;
    }

    @Override
    public Void visitConditionInConstantSet(ConditionInConstantSet e) {
        int level = this.level;
        super.visitConditionInConstantSet(e);
        e.setQueryLevel(level);
        return null;
    }

    @Override
    public Void visitConditionInSelect(ConditionInSelect e) {
        e.getLeft().accept(this);
        level++;
        visitQuery(e.getQuery());
        level--;
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        // 聚合函数不能嵌套
        if (resolver.getState() == ColumnResolver.STATE_IN_AGGREGATE) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, e.getSQL());
        }
        if (e.getOn() != null) {
            int state = resolver.getState();
            resolver.setState(ColumnResolver.STATE_IN_AGGREGATE);
            try {
                super.visitAggregate(e);
            } finally {
                resolver.setState(state);
            }
        }
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        // 聚合函数不能嵌套
        if (resolver.getState() == ColumnResolver.STATE_IN_AGGREGATE) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, e.getSQL());
        }
        int state = resolver.getState();
        resolver.setState(ColumnResolver.STATE_IN_AGGREGATE);
        try {
            super.visitAGroupConcat(e);
        } finally {
            resolver.setState(state);
        }
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        // 聚合函数不能嵌套
        if (resolver.getState() == ColumnResolver.STATE_IN_AGGREGATE) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, e.getSQL());
        }
        int state = resolver.getState();
        resolver.setState(ColumnResolver.STATE_IN_AGGREGATE);
        try {
            super.visitJavaAggregate(e);
        } finally {
            resolver.setState(state);
        }
        return null;
    }
}
