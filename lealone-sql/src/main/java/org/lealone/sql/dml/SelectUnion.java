/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.ISelectUnion;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.yieldable.DefaultYieldableQuery;
import org.lealone.sql.yieldable.YieldableBase;
import org.lealone.sql.yieldable.YieldableQueryBase;

/**
 * Represents a union SELECT statement.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SelectUnion extends Query implements ISelectUnion {

    private int unionType;
    private final Query left;
    private Query right;

    public SelectUnion(ServerSession session, Query query) {
        super(session);
        this.left = query;
    }

    @Override
    public int getType() {
        return SQLStatement.SELECT;
    }

    public void setUnionType(int type) {
        this.unionType = type;
    }

    @Override
    public int getUnionType() {
        return unionType;
    }

    public void setRight(Query select) {
        right = select;
    }

    @Override
    public Query getRight() {
        return right;
    }

    @Override
    public Query getLeft() {
        return left;
    }

    @Override
    public Result getMetaData() {
        int columnCount = left.getColumnCount();
        LocalResult result = new LocalResult(session, expressionArray, columnCount);
        result.done();
        return result;
    }

    @Override
    public LocalResult getEmptyResult() {
        int columnCount = left.getColumnCount();
        return new LocalResult(session, expressionArray, columnCount);
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        checkInit = true;
        left.init();
        right.init();
        int len = left.getColumnCount();
        if (len != right.getColumnCount()) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        ArrayList<Expression> le = left.getExpressions();
        // set the expressions to get the right column count and names,
        // but can't validate at this time
        expressions = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            expressions.add(l);
        }
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return this;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        isPrepared = true;
        left.prepare();
        right.prepare();
        int len = left.getColumnCount();
        // set the correct expressions now
        expressions = new ArrayList<>(len);
        ArrayList<Expression> le = left.getExpressions();
        ArrayList<Expression> re = right.getExpressions();
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            Expression r = re.get(i);
            int type = Value.getHigherOrder(l.getType(), r.getType());
            long prec = Math.max(l.getPrecision(), r.getPrecision());
            int scale = Math.max(l.getScale(), r.getScale());
            int displaySize = Math.max(l.getDisplaySize(), r.getDisplaySize());
            Column col = new Column(l.getAlias(), type, prec, scale, displaySize);
            Expression e = new ExpressionColumn(session.getDatabase(), col);
            expressions.add(e);
        }
        if (orderList != null) {
            initOrder(session, expressions, null, orderList, getColumnCount(), true, null);
            sort = prepareOrder(session, orderList, expressions.size());
            orderList = null;
        }
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
        return this;
    }

    private Value[] convert(Value[] values, int columnCount) {
        Value[] newValues;
        if (columnCount == values.length) {
            // re-use the array if possible
            newValues = values;
        } else {
            // create a new array if needed,
            // for the value hash set
            newValues = new Value[columnCount];
        }
        for (int i = 0; i < columnCount; i++) {
            Expression e = expressions.get(i);
            newValues[i] = values[i].convertTo(e.getType());
        }
        return newValues;
    }

    @Override
    public double getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = left.getTables();
        set.addAll(right.getTables());
        return set;
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        left.setForUpdate(forUpdate);
        right.setForUpdate(forUpdate);
        isForUpdate = forUpdate;
    }

    @Override
    public int getColumnCount() {
        return left.getColumnCount();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        switch (unionType) {
        case UNION_ALL:
        case UNION:
        case INTERSECT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            right.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        case EXCEPT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
    }

    @Override
    public String getPlanSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getPlanSQL()).append(')');
        switch (unionType) {
        case UNION_ALL:
            buff.append("\nUNION ALL\n");
            break;
        case UNION:
            buff.append("\nUNION\n");
            break;
        case INTERSECT:
            buff.append("\nINTERSECT\n");
            break;
        case EXCEPT:
            buff.append("\nEXCEPT\n");
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        buff.append('(').append(right.getPlanSQL()).append(')');
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, exprList.length));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append("\nOFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public void updateAggregate(ServerSession s) {
        left.updateAggregate(s);
        right.updateAggregate(s);
    }

    @Override
    public void fireBeforeSelectTriggers() {
        left.fireBeforeSelectTriggers();
        right.fireBeforeSelectTriggers();
    }

    @Override
    public boolean allowGlobalConditions() {
        return left.allowGlobalConditions() && right.allowGlobalConditions();
    }

    @Override
    public List<TableFilter> getTopFilters() {
        List<TableFilter> filters = left.getTopFilters();
        filters.addAll(right.getTopFilters());
        return filters;
    }

    @Override
    public void addGlobalCondition(CommandParameter param, int columnId, int indexConditionType) {
        this.addGlobalCondition((Parameter) param, columnId, indexConditionType);
    }

    @Override
    public int getPriority() {
        return Math.min(left.getPriority(), left.getPriority());
    }

    @Override
    public Result query(int maxRows, ResultTarget target) {
        YieldableSelectUnion yieldable = new YieldableSelectUnion(this, maxRows, false, null, target);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        if (!isLocal() && getSession().isShardingMode())
            return new DefaultYieldableQuery(this, maxRows, scrollable, asyncHandler);
        else
            return new YieldableSelectUnion(this, maxRows, scrollable, asyncHandler, target);
    }

    private class YieldableSelectUnion extends YieldableQueryBase {

        private final ResultTarget target;
        Expression limitExpr;
        boolean insertFromSelect;
        YieldableBase<Result> leftYieldableQuery;
        YieldableBase<Result> rightYieldableQuery;
        int columnCount;
        LocalResult result;
        Result leftRows;
        Result rightRows;
        LocalResult temp;
        int rowNumber;
        boolean done;

        public YieldableSelectUnion(SelectUnion statement, int maxRows, boolean scrollable,
                AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
            super(statement, maxRows, scrollable, asyncHandler);
            this.target = target;
        }

        @Override
        protected boolean startInternal() {
            fireBeforeSelectTriggers();
            // union doesn't always know the parameter list of the left and right queries
            if (maxRows != 0) {
                // maxRows is set (maxRows 0 means no limit)
                int l;
                if (limitExpr == null) {
                    l = -1;
                } else {
                    Value v = limitExpr.getValue(session);
                    l = v == ValueNull.INSTANCE ? -1 : v.getInt();
                }
                if (l < 0) {
                    // for limitExpr, 0 means no rows, and -1 means no limit
                    l = maxRows;
                } else {
                    l = Math.min(l, maxRows);
                }
                limitExpr = ValueExpression.get(ValueInt.get(l));
            }

            if (session.getDatabase().getSettings().optimizeInsertFromSelect) {
                if (unionType == UNION_ALL && target != null) {
                    if (sort == null && !distinct && maxRows == 0 && offsetExpr == null && limitExpr == null) {
                        insertFromSelect = true;
                        leftYieldableQuery = left.createYieldableQuery(0, false, null, target);
                        rightYieldableQuery = right.createYieldableQuery(0, false, null, target);
                        return false;
                    }
                }
            }
            columnCount = left.getColumnCount();
            result = new LocalResult(session, expressionArray, columnCount);
            if (sort != null) {
                result.setSortOrder(sort);
            }
            if (distinct) {
                left.setDistinct(true);
                right.setDistinct(true);
                result.setDistinct();
            }
            if (randomAccessResult) {
                result.setRandomAccess();
            }
            switch (unionType) {
            case UNION:
            case EXCEPT:
                left.setDistinct(true);
                right.setDistinct(true);
                result.setDistinct();
                break;
            case UNION_ALL:
                break;
            case INTERSECT:
                left.setDistinct(true);
                right.setDistinct(true);
                temp = new LocalResult(session, expressionArray, columnCount);
                temp.setDistinct();
                temp.setRandomAccess();
                break;
            default:
                DbException.throwInternalError("type=" + unionType);
            }
            leftYieldableQuery = left.createYieldableQuery(0, false, null, null);
            rightYieldableQuery = right.createYieldableQuery(0, false, null, null);
            return false;
        }

        @Override
        protected void stopInternal() {
        }

        @Override
        protected void executeInternal() {
            if (insertFromSelect) {
                if (leftYieldableQuery != null) {
                    leftYieldableQuery.run();
                    if (leftYieldableQuery.isStopped()) {
                        leftYieldableQuery = null;
                    }
                }
                if (leftYieldableQuery == null && rightYieldableQuery != null) {
                    rightYieldableQuery.run();
                    if (rightYieldableQuery.isStopped()) {
                        rightYieldableQuery = null;
                    }
                }
            }

            switch (unionType) {
            case UNION_ALL:
            case UNION: {
                if (leftYieldableQuery != null && runLeftQuery()) {
                    return;
                }
                if (leftRows != null && addLeftRows()) {
                    return;
                }

                if (rightYieldableQuery != null && runRightQuery()) {
                    return;
                }
                if (rightRows != null && addRightRows()) {
                    return;
                }
                break;
            }
            case EXCEPT: {
                if (leftYieldableQuery != null && runLeftQuery()) {
                    return;
                }
                if (leftRows != null && addLeftRows()) {
                    return;
                }

                if (rightYieldableQuery != null && runRightQuery()) {
                    return;
                }
                if (rightRows != null) {
                    while (rightRows.next()) {
                        result.removeDistinct(convert(rightRows.currentRow(), columnCount));
                        if (yieldIfNeeded(++rowNumber)) {
                            return;
                        }
                    }
                    rightRows = null;
                }
                break;
            }
            case INTERSECT: {
                if (leftYieldableQuery != null && runLeftQuery()) {
                    return;
                }
                if (leftRows != null) {
                    while (leftRows.next()) {
                        temp.addRow(convert(leftRows.currentRow(), columnCount));
                        if (yieldIfNeeded(++rowNumber)) {
                            return;
                        }
                    }
                    leftRows = null;
                }

                if (rightYieldableQuery != null && runRightQuery()) {
                    return;
                }
                if (rightRows != null) {
                    while (rightRows.next()) {
                        Value[] values = convert(rightRows.currentRow(), columnCount);
                        if (temp.containsDistinct(values)) {
                            result.addRow(values);
                        }
                        if (yieldIfNeeded(++rowNumber)) {
                            return;
                        }
                    }
                    rightRows = null;
                }
                break;
            }
            default:
                DbException.throwInternalError("type=" + unionType);
            }
            if (!done) {
                if (offsetExpr != null) {
                    result.setOffset(offsetExpr.getValue(session).getInt());
                }
                if (limitExpr != null) {
                    Value v = limitExpr.getValue(session);
                    if (v != ValueNull.INSTANCE) {
                        result.setLimit(v.getInt());
                    }
                }
                rowNumber = 0;
                result.done();
                done = true;
            }
            if (target == null) {
                setResult(result, result.getRowCount());
            } else {
                while (result.next()) {
                    target.addRow(result.currentRow());
                    if (yieldIfNeeded(++rowNumber)) {
                        return;
                    }
                }
                result.close();
            }
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }

        private boolean runLeftQuery() {
            if (leftRows == null) {
                leftYieldableQuery.run();
                if (leftYieldableQuery.isStopped()) {
                    rowNumber = 0;
                    leftRows = leftYieldableQuery.getResult();
                    leftYieldableQuery = null;
                    return false;
                }
            }
            return true;
        }

        private boolean runRightQuery() {
            if (rightRows == null) {
                rightYieldableQuery.run();
                if (rightYieldableQuery.isStopped()) {
                    rowNumber = 0;
                    rightRows = rightYieldableQuery.getResult();
                    rightYieldableQuery = null;
                    return false;
                }
            }
            return true;
        }

        private boolean addLeftRows() {
            while (leftRows.next()) {
                result.addRow(convert(leftRows.currentRow(), columnCount));
                if (yieldIfNeeded(++rowNumber)) {
                    return true;
                }
            }
            leftRows = null;
            return false;
        }

        private boolean addRightRows() {
            while (rightRows.next()) {
                result.addRow(convert(rightRows.currentRow(), columnCount));
                if (yieldIfNeeded(++rowNumber)) {
                    return true;
                }
            }
            rightRows = null;
            return false;
        }
    }
}
