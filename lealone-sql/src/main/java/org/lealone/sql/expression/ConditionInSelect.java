/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.expression.ExpressionVisitor;
import org.lealone.db.index.IndexCondition;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.dml.Query;

/**
 * An 'in' condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public class ConditionInSelect extends Condition {

    private final Database database;
    private Expression left;
    private final Query query;
    private final boolean all;
    private final int compareType;
    private int queryLevel;

    public ConditionInSelect(Database database, Expression left, Query query, boolean all, int compareType) {
        this.database = database;
        this.left = left;
        this.query = query;
        this.all = all;
        this.compareType = compareType;
    }

    @Override
    public Value getValue(ServerSession session) {
        query.setSession(session);
        SubqueryResult rows = new SubqueryResult(query, 0); // query.query(0);
        session.addTemporaryResult(rows);
        Value l = left.getValue(session);
        if (rows.getRowCount() == 0) {
            return ValueBoolean.get(all);
        } else if (l == ValueNull.INSTANCE) {
            return l;
        }
        if (!session.getDatabase().getSettings().optimizeInSelect) {
            return getValueSlow(rows, l);
        }
        if (all || (compareType != Comparison.EQUAL && compareType != Comparison.EQUAL_NULL_SAFE)) {
            return getValueSlow(rows, l);
        }
        int dataType = rows.getColumnType(0);
        if (dataType == Value.NULL) {
            return ValueBoolean.get(false);
        }
        l = l.convertTo(dataType);
        if (rows.containsDistinct(new Value[] { l })) {
            return ValueBoolean.get(true);
        }
        if (rows.containsDistinct(new Value[] { ValueNull.INSTANCE })) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(false);
    }

    private Value getValueSlow(SubqueryResult rows, Value l) {
        // this only returns the correct result if the result has at least one
        // row, and if l is not null
        boolean hasNull = false;
        boolean result = all;
        while (rows.next()) {
            boolean value;
            Value r = rows.currentRow()[0];
            if (r == ValueNull.INSTANCE) {
                value = false;
                hasNull = true;
            } else {
                value = Comparison.compareNotNull(database, l, r, compareType);
            }
            if (!value && all) {
                result = false;
                break;
            } else if (value && !all) {
                result = true;
                break;
            }
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        query.mapColumns(resolver, level + 1);
        this.queryLevel = Math.max(level, this.queryLevel);
    }

    @Override
    public Expression optimize(ServerSession session) {
        left = left.optimize(session);
        query.setRandomAccessResult(true);
        query.prepare();
        if (query.getColumnCount() != 1) {
            throw DbException.get(ErrorCode.SUBQUERY_IS_NOT_SINGLE_COLUMN);
        }
        // Can not optimize: the data may change
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public String getSQL(boolean isDistributed) {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getSQL(isDistributed)).append(' ');
        if (all) {
            buff.append(Comparison.getCompareOperator(compareType)).append(" ALL");
        } else {
            if (compareType != Comparison.EQUAL)
                buff.append(Comparison.getCompareOperator(compareType)).append(" SOME");
            else
                buff.append("IN");
        }
        buff.append("(\n").append(StringUtils.indent(query.getPlanSQL(), 4, false)).append("))");
        return buff.toString();
    }

    @Override
    public void updateAggregate(ServerSession session) {
        left.updateAggregate(session);
        query.updateAggregate(session);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + query.getCostAsExpression();
    }

    @Override
    public void createIndexConditions(ServerSession session, TableFilter filter) {
        if (!session.getDatabase().getSettings().optimizeInList) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        if (!query.isEverything(visitor)) {
            return;
        }
        filter.addIndexCondition(IndexCondition.getInQuery(l, query));
    }

}
