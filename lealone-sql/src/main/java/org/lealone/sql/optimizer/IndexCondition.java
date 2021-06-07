/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.index.IndexConditionType;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.TableType;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.query.Query;

/**
 * A index condition object is made for each condition that can potentially use
 * an index. This class does not extend expression, but in general there is one
 * expression that maps to each index condition.
 */
public class IndexCondition {

    private final Column column;

    /**
     * see constants in {@link Comparison}
     */
    private final int compareType;

    private final Expression expression;
    private List<? extends Expression> expressionList;
    private Query expressionQuery;

    /**
     * @param compareType the comparison type, see constants in {@link Comparison}
     */
    private IndexCondition(int compareType, ExpressionColumn column, Expression expression) {
        this.compareType = compareType;
        this.column = column == null ? null : column.getColumn();
        this.expression = expression;
    }

    /**
     * Create an index condition with the given parameters.
     *
     * @param compareType the comparison type, see constants in {@link Comparison}
     * @param column the column
     * @param expression the expression
     * @return the index condition
     */
    public static IndexCondition get(int compareType, ExpressionColumn column, Expression expression) {
        return new IndexCondition(compareType, column, expression);
    }

    /**
     * Create an index condition with the compare type IN_LIST and with the
     * given parameters.
     *
     * @param column the column
     * @param list the expression list
     * @return the index condition
     */
    public static IndexCondition getInList(ExpressionColumn column, List<? extends Expression> list) {
        IndexCondition cond = new IndexCondition(Comparison.IN_LIST, column, null);
        cond.expressionList = list;
        return cond;
    }

    /**
     * Create an index condition with the compare type IN_QUERY and with the
     * given parameters.
     *
     * @param column the column
     * @param query the select statement
     * @return the index condition
     */
    public static IndexCondition getInQuery(ExpressionColumn column, Query query) {
        IndexCondition cond = new IndexCondition(Comparison.IN_QUERY, column, null);
        cond.expressionQuery = query;
        return cond;
    }

    /**
     * Get the current value of the expression.
     *
     * @param session the session
     * @return the value
     */
    public Value getCurrentValue(ServerSession session) {
        return expression.getValue(session);
    }

    /**
     * Get the current value list of the expression. The value list is of the
     * same type as the column, distinct, and sorted.
     *
     * @param session the session
     * @return the value list
     */
    public Value[] getCurrentValueList(ServerSession session) {
        HashSet<Value> valueSet = new HashSet<Value>();
        for (Expression e : expressionList) {
            Value v = e.getValue(session);
            v = column.convert(v);
            valueSet.add(v);
        }
        Value[] array = new Value[valueSet.size()];
        valueSet.toArray(array);
        final CompareMode mode = session.getDatabase().getCompareMode();
        Arrays.sort(array, new Comparator<Value>() {
            @Override
            public int compare(Value o1, Value o2) {
                return o1.compareTo(o2, mode);
            }
        });
        return array;
    }

    /**
     * Get the current result of the expression. The rows may not be of the same
     * type, therefore the rows may not be unique.
     *
     * @return the result
     */
    public Result getCurrentResult() {
        return expressionQuery.query(0);
    }

    /**
     * Get the SQL snippet of this comparison.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        if (compareType == Comparison.FALSE) {
            return "FALSE";
        }
        StatementBuilder buff = new StatementBuilder();
        buff.append(column.getSQL());
        switch (compareType) {
        case Comparison.EQUAL:
            buff.append(" = ");
            break;
        case Comparison.EQUAL_NULL_SAFE:
            buff.append(" IS ");
            break;
        case Comparison.BIGGER_EQUAL:
            buff.append(" >= ");
            break;
        case Comparison.BIGGER:
            buff.append(" > ");
            break;
        case Comparison.SMALLER_EQUAL:
            buff.append(" <= ");
            break;
        case Comparison.SMALLER:
            buff.append(" < ");
            break;
        case Comparison.IN_LIST:
            buff.append(" IN(");
            for (Expression e : expressionList) {
                buff.appendExceptFirst(", ");
                buff.append(e.getSQL());
            }
            buff.append(')');
            break;
        case Comparison.IN_QUERY:
            buff.append(" IN(");
            buff.append(expressionQuery.getPlanSQL());
            buff.append(')');
            break;
        default:
            DbException.throwInternalError("type=" + compareType);
        }
        if (expression != null) {
            buff.append(expression.getSQL());
        }
        return buff.toString();
    }

    /**
     * Get the comparison bit mask.
     *
     * @param indexConditions all index conditions
     * @return the mask
     */
    public int getMask(ArrayList<IndexCondition> indexConditions) {
        switch (compareType) {
        case Comparison.FALSE:
            return IndexConditionType.ALWAYS_FALSE;
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
            return IndexConditionType.EQUALITY;
        case Comparison.IN_LIST:
        case Comparison.IN_QUERY:
            if (indexConditions.size() > 1) {
                if (column.getTable().getTableType() != TableType.STANDARD_TABLE) {
                    // if combined with other conditions,
                    // IN(..) can only be used for regular tables
                    // test case:
                    // create table test(a int, b int, primary key(id, name));
                    // create unique index c on test(b, a);
                    // insert into test values(1, 10), (2, 20);
                    // select * from (select * from test)
                    // where a=1 and b in(10, 20);
                    return 0;
                }
            }
            return IndexConditionType.EQUALITY;
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return IndexConditionType.START;
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return IndexConditionType.END;
        default:
            throw DbException.getInternalError("type=" + compareType);
        }
    }

    /**
     * Check if the result is always false.
     *
     * @return true if the result will always be false
     */
    public boolean isAlwaysFalse() {
        return compareType == Comparison.FALSE;
    }

    /**
     * Check if this index condition is of the type column larger or equal to
     * value.
     *
     * @return true if this is a start condition
     */
    public boolean isStart() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if this index condition is of the type column smaller or equal to
     * value.
     *
     * @return true if this is a end condition
     */
    public boolean isEnd() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return true;
        default:
            return false;
        }
    }

    public int getCompareType() {
        return compareType;
    }

    /**
     * Get the referenced column.
     *
     * @return the column
     */
    public Column getColumn() {
        return column;
    }

    /**
     * Check if the expression can be evaluated.
     *
     * @return true if it can be evaluated
     */
    public boolean isEvaluatable() {
        if (expression != null) {
            return expression.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR);
        }
        if (expressionList != null) {
            for (Expression e : expressionList) {
                if (!e.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    return false;
                }
            }
            return true;
        }
        return expressionQuery.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR);
    }

}
