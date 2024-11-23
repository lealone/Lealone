/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.command.CommandParameter;
import com.lealone.db.index.IndexConditionType;
import com.lealone.db.result.Result;
import com.lealone.db.result.ResultTarget;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.dml.ManipulationStatement;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.expression.Alias;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.SelectOrderBy;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.expression.visitor.ExpressionVisitorFactory;
import com.lealone.sql.expression.visitor.MaxModificationIdVisitor;
import com.lealone.sql.optimizer.ColumnResolver;
import com.lealone.sql.optimizer.TableFilter;

/**
 * Represents a SELECT statement (simple, or union).
 */
public abstract class Query extends ManipulationStatement implements com.lealone.sql.IQuery {

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    protected Expression limitExpr;

    /** 
     * The offset expression as specified in the LIMIT .. OFFSET clause.
     */
    protected Expression offsetExpr;

    /**
     * The sample size expression as specified in the SAMPLE_SIZE clause.
     */
    protected Expression sampleSizeExpr;

    /**
     * Whether the result must only contain distinct rows.
     */
    protected boolean distinct;

    // 存放原始表达式的Alias和ColumnName，用于给客户端返回最原始的信息
    protected ArrayList<String[]> rawExpressionInfoList;
    protected ArrayList<Expression> expressions;
    protected Expression[] expressionArray;
    protected ArrayList<SelectOrderBy> orderList;
    protected SortOrder sort;
    protected boolean isPrepared, checkInit;
    protected boolean isForUpdate;

    Query(ServerSession session) {
        super(session);
    }

    /**
     * Initialize the query.
     */
    public abstract void init();

    /**
     * The the list of select expressions.
     * This may include invisible expressions such as order by expressions.
     *
     * @return the list of expressions
     */
    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    /**
     * Calculate the cost to execute this query.
     *
     * @return the cost
     */
    @Override
    public abstract double getCost();

    /**
     * Calculate the cost when used as a subquery.
     * This method returns a value between 10 and 1000000,
     * to ensure adding other values can't result in an integer overflow.
     *
     * @return the estimated cost as an integer
     */
    public int getCostAsExpression() {
        // ensure the cost is not larger than 1 million,
        // so that adding other values can't overflow
        return (int) Math.min(1000000.0, 10.0 + 10.0 * getCost());
    }

    /**
     * Get all tables that are involved in this query.
     *
     * @return the set of tables
     */
    @Override
    public abstract HashSet<Table> getTables();

    /**
     * Set the order by list.
     *
     * @param order the order by list
     */
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    /**
     * Set the 'for update' flag.
     *
     * @param forUpdate the new setting
     */
    public abstract void setForUpdate(boolean forUpdate);

    /**
     * Get the column count of this query.
     *
     * @return the column count
     */
    @Override
    public abstract int getColumnCount();

    /**
     * Map the columns to the given column resolver.
     *
     * @param resolver
     *            the resolver
     * @param level
     *            the subquery level (0 is the top level query, 1 is the first
     *            subquery level)
     */
    public abstract void mapColumns(ColumnResolver resolver, int level);

    /**
     * Check whether adding condition to the query is allowed. This is not
     * allowed for views that have an order by and a limit, as it would affect
     * the returned results.
     *
     * @return true if adding global conditions is allowed
     */
    @Override
    public abstract boolean allowGlobalConditions();

    @Override
    public void addGlobalCondition(CommandParameter param, int columnId, int indexConditionType) {
        int comparisonType = 0;
        switch (indexConditionType) {
        case IndexConditionType.EQUALITY:
            comparisonType = Comparison.EQUAL_NULL_SAFE;
            break;
        case IndexConditionType.START:
            comparisonType = Comparison.BIGGER_EQUAL;
            break;
        case IndexConditionType.END:
            comparisonType = Comparison.SMALLER_EQUAL;
            break;
        default:
            throw DbException.getInternalError("indexConditionType: " + indexConditionType);
        }
        this.addGlobalCondition((Parameter) param, columnId, comparisonType);
    }

    /**
     * Add a condition to the query. This is used for views.
     *
     * @param param the parameter
     * @param columnId the column index (0 meaning the first column)
     * @param comparisonType the comparison type
     */
    public abstract void addGlobalCondition(Parameter param, int columnId, int comparisonType);

    public abstract <R> R accept(ExpressionVisitor<R> visitor);

    /**
     * Call the before triggers on all tables.
     */
    public abstract void fireBeforeSelectTriggers();

    /**
     * Set the distinct flag.
     *
     * @param b the new value
     */
    public void setDistinct(boolean b) {
        distinct = b;
    }

    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    /**
     * Disable caching of result sets.
     */
    @Override
    public void disableCache() {
        // do nothing
    }

    @Override
    public Result query(int maxRows) {
        return query(maxRows, null);
    }

    /**
     * Execute the query, writing the result to the target result.
     *
     * @param maxRows the maximum number of rows to return
     * @param target the target result (null will return the result)
     * @return the result set (if the target is not set).
     */
    public abstract Result query(int maxRows, ResultTarget target);

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncResultHandler<Result> asyncHandler) {
        return createYieldableQuery(maxRows, scrollable, asyncHandler, null);
    }

    public abstract YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncResultHandler<Result> asyncHandler, ResultTarget target);

    public void setOffset(Expression offset) {
        this.offsetExpr = offset;
    }

    public Expression getOffset() {
        return offsetExpr;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    public Expression getLimit() {
        return limitExpr;
    }

    /**
     * Add a parameter to the parameter list.
     *
     * @param param the parameter to add
     */
    void addParameter(Parameter param) {
        if (parameters == null) {
            parameters = new ArrayList<>();
        }
        parameters.add(param);
    }

    public void setSampleSize(Expression sampleSize) {
        this.sampleSizeExpr = sampleSize;
    }

    /**
     * Get the sample size, if set.
     *
     * @param session the session
     * @return the sample size
     */
    int getSampleSizeValue(ServerSession session) {
        if (sampleSizeExpr == null) {
            return 0;
        }
        Value v = sampleSizeExpr.optimize(session).getValue(session);
        if (v == ValueNull.INSTANCE) {
            return 0;
        }
        return v.getInt();
    }

    @Override
    public final long getMaxDataModificationId() {
        MaxModificationIdVisitor visitor = ExpressionVisitorFactory.getMaxModificationIdVisitor();
        accept(visitor);
        return visitor.getMaxDataModificationId();
    }

    public abstract List<TableFilter> getFilters();

    public abstract List<TableFilter> getTopFilters();

    @Override
    public boolean isDeterministic() {
        return accept(ExpressionVisitorFactory.getDeterministicVisitor());
    }

    /**
    * Initialize the order by list. This call may extend the expressions list.
    *
    * @param session the session
    * @param expressions the select list expressions
    * @param expressionSQL the select list SQL snippets
    * @param orderList the order by list
    * @param visible the number of visible columns in the select list
    * @param mustBeInResult all order by expressions must be in the select list
    * @param filters the table filters
    */
    static void initOrder(ServerSession session, ArrayList<Expression> expressions,
            ArrayList<String> expressionSQL, ArrayList<SelectOrderBy> orderList, int visible,
            boolean mustBeInResult, ArrayList<TableFilter> filters) {
        Database db = session.getDatabase();
        for (SelectOrderBy o : orderList) {
            Expression e = o.expression;
            if (e == null) {
                continue;
            }
            // special case: SELECT 1 AS A FROM DUAL ORDER BY A
            // (oracle supports it, but only in order by, not in group by and
            // not in having):
            // SELECT 1 AS A FROM DUAL ORDER BY -A
            boolean isAlias = false;
            int idx = expressions.size();
            if (e instanceof ExpressionColumn) {
                // order by expression
                ExpressionColumn exprCol = (ExpressionColumn) e;
                String tableAlias = exprCol.getOriginalTableAliasName();
                String col = exprCol.getOriginalColumnName();
                for (int j = 0; j < visible; j++) {
                    boolean found = false;
                    Expression ec = expressions.get(j);
                    if (ec instanceof ExpressionColumn) {
                        // select expression
                        ExpressionColumn c = (ExpressionColumn) ec;
                        found = db.equalsIdentifiers(col, c.getColumnName());
                        if (found && tableAlias != null) {
                            String ca = c.getOriginalTableAliasName();
                            if (ca == null) {
                                found = false;
                                if (filters != null) {
                                    // select id from test order by test.id
                                    for (int i = 0, size = filters.size(); i < size; i++) {
                                        TableFilter f = filters.get(i);
                                        if (db.equalsIdentifiers(f.getTableAlias(), tableAlias)) {
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            } else {
                                found = db.equalsIdentifiers(ca, tableAlias);
                            }
                        }
                    } else if (!(ec instanceof Alias)) {
                        continue;
                    } else if (tableAlias == null && db.equalsIdentifiers(col, ec.getAlias())) {
                        found = true;
                    } else {
                        Expression ec2 = ec.getNonAliasExpression();
                        if (ec2 instanceof ExpressionColumn) {
                            ExpressionColumn c2 = (ExpressionColumn) ec2;
                            String ta = exprCol.getSQL();
                            String tb = c2.getSQL();
                            String s2 = c2.getColumnName();
                            found = db.equalsIdentifiers(col, s2);
                            if (!db.equalsIdentifiers(ta, tb)) {
                                found = false;
                            }
                        }
                    }
                    if (found) {
                        idx = j;
                        isAlias = true;
                        break;
                    }
                }
            } else {
                String s = e.getSQL();
                if (expressionSQL != null) {
                    for (int j = 0, size = expressionSQL.size(); j < size; j++) {
                        String s2 = expressionSQL.get(j);
                        if (db.equalsIdentifiers(s2, s)) {
                            idx = j;
                            isAlias = true;
                            break;
                        }
                    }
                }
            }
            if (!isAlias) {
                if (mustBeInResult) {
                    throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, e.getSQL());
                }
                expressions.add(e);
                String sql = e.getSQL();
                expressionSQL.add(sql);
            }
            o.columnIndexExpr = ValueExpression.get(ValueInt.get(idx + 1));
            Expression expr = expressions.get(idx).getNonAliasExpression();
            o.expression = expr;
        }
    }

    /**
    * Create a {@link SortOrder} object given the list of {@link SelectOrderBy}
    * objects. The expression list is extended if necessary.
    *
    * @param session the session
    * @param orderList a list of {@link SelectOrderBy} elements
    * @param expressionCount the number of columns in the query
    * @return the {@link SortOrder} object
    */
    static SortOrder prepareOrder(ServerSession session, ArrayList<SelectOrderBy> orderList,
            int expressionCount) {
        int size = orderList.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        Column[] orderColumns = new Column[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderList.get(i);
            int idx;
            boolean reverse = false;
            Expression expr = o.columnIndexExpr;
            Value v = expr.getValue(null);
            if (v == ValueNull.INSTANCE) {
                // parameter not yet set - order by first column
                idx = 0;
            } else {
                idx = v.getInt();
                if (idx < 0) {
                    reverse = true;
                    idx = -idx;
                }
                idx -= 1;
                if (idx < 0 || idx >= expressionCount) {
                    throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, "" + (idx + 1));
                }
            }
            index[i] = idx;
            boolean desc = o.descending;
            if (reverse) {
                desc = !desc;
            }
            int type = desc ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            if (o.nullsFirst) {
                type += SortOrder.NULLS_FIRST;
            } else if (o.nullsLast) {
                type += SortOrder.NULLS_LAST;
            }
            sortType[i] = type;
            orderColumns[i] = getOrderColumn(orderList, i);
        }
        return new SortOrder(session.getDatabase(), index, sortType, orderColumns);
    }

    private static Column getOrderColumn(ArrayList<SelectOrderBy> orderList, int index) {
        SelectOrderBy order = orderList.get(index);
        Expression expr = order.expression;
        if (expr == null) {
            return null;
        }
        expr = expr.getNonAliasExpression();
        if (expr.isConstant()) {
            return null;
        }
        if (!(expr instanceof ExpressionColumn)) {
            return null;
        }
        ExpressionColumn exprCol = (ExpressionColumn) expr;
        return exprCol.getColumn();
    }
}
