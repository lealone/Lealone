/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.New;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexConditionType;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.Table;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Calculator;
import org.lealone.sql.expression.Comparison;
import org.lealone.sql.expression.ConditionAndOr;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.Optimizer;
import org.lealone.sql.optimizer.TableFilter;

/**
 * This class represents a simple SELECT statement.
 *
 * For each select statement,
 * visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount.
 * The expression list count could include ORDER BY and GROUP BY expressions
 * that are not in the select list.
 *
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {
    private TableFilter topTableFilter;
    private final ArrayList<TableFilter> filters = New.arrayList();
    private final ArrayList<TableFilter> topFilters = New.arrayList();
    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ArrayList<SelectOrderBy> orderList;
    private ArrayList<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private HashMap<Expression, Object> currentGroup;
    private int havingIndex;
    private boolean isGroupQuery, isGroupSortedQuery;
    private boolean isForUpdate, isForUpdateMvcc;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery, isDistinctQueryForMultiFields;
    private boolean isPrepared, checkInit;
    private boolean sortUsingIndex;
    private SortOrder sort;
    private int currentGroupRowId;

    public Select(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.SELECT;
    }

    @Override
    public boolean isCacheable() {
        return !isForUpdate;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    @Override
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    public int getLimitRows() {
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            return v == ValueNull.INSTANCE ? -1 : v.getInt();
        } else
            return -1;
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    @Override
    public ArrayList<TableFilter> getTopFilters() {
        return topFilters;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ArrayList<String> expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = New.arrayList();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(session, expressions, expressionSQL, orderList, visibleColumnCount, distinct, filters);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            Database db = session.getDatabase();
            int size = group.size();
            int expSize = expressionSQL.size();
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                        sql = expr.getAlias();
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            mapColumns(f, 0);
        }
        checkInit = true;
    }

    private void expandColumnList() {
        Database db = session.getDatabase();

        // the expressions may change within the loop
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            // select mytable.* from mytable as t这种用法是错的，MySQL也报错
            // 必须这样select t.* from mytable as t或者select mytable.* from mytable
            // 这里的tableName有可能是mytable也可能是t
            String tableName = expr.getTableName();
            if (tableName == null) { // select *，展开所有表中的字段
                expressions.remove(i);
                for (TableFilter filter : filters) {
                    i = expandColumnList(filter, i);
                }
                i--;
            } else { // select s.t.*或select t.*，展开指定模式和指定表中的字段
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    // 如果没有指定别名，f.getTableAlias()就返回最初的表名
                    if (db.equalsIdentifiers(tableName, f.getTableAlias())) {
                        if (schemaName == null || db.equalsIdentifiers(schemaName, f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
                }
                expressions.remove(i);
                i = expandColumnList(filter, i);
                i--;
            }
        }
    }

    private int expandColumnList(TableFilter filter, int index) {
        String alias = filter.getTableAlias();
        Column[] columns = filter.getTable().getColumns();
        for (Column c : columns) {
            // 跳过Natural Join列，
            // 右边的表对应的TableFilter有Natural Join列，而左边没有
            if (filter.isNaturalJoinColumn(c)) {
                continue;
            }
            ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), null, alias, c.getName());
            expressions.add(index++, ec);
        }
        return index;
    }

    @Override
    public PreparedStatement prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return this;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(session, orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            for (TableFilter f : filters) {
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
        }

        // 对min、max、count三个聚合函数的特殊优化
        if (condition == null && isGroupQuery && groupIndex == null && havingIndex < 0 && filters.size() == 1
                && filters.get(0).getPageKeys() != null) {
            Table t = filters.get(0).getTable();
            ExpressionVisitor optimizable = ExpressionVisitor.getOptimizableVisitor(t);
            isQuickAggregateQuery = isEverything(optimizable);
        }

        cost = preparePlan(); // 选择合适的索引

        // 以下3个if为特殊的distinct、sort、group by选择更合适的索引
        // 1. distinct
        if (distinct && session.getDatabase().getSettings().optimizeDistinct && !isGroupQuery && filters.size() == 1
                && condition == null) {
            optimizeDistinct();
        }
        // 2. sort
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            Index index = getSortIndex();
            if (index != null) {
                Index current = topTableFilter.getIndex();
                if (current.getIndexType().isScan() || current == index) {
                    topTableFilter.setIndex(index);
                    if (!topTableFilter.hasInComparisons()) {
                        // in(select ...) and in(1,2,3) may return the key in
                        // another order
                        sortUsingIndex = true;
                    }
                } else if (index.getIndexColumns().length >= current.getIndexColumns().length) {
                    IndexColumn[] sortColumns = index.getIndexColumns();
                    IndexColumn[] currentColumns = current.getIndexColumns();
                    boolean swapIndex = false;
                    for (int i = 0; i < currentColumns.length; i++) {
                        if (sortColumns[i].column != currentColumns[i].column) {
                            swapIndex = false;
                            break;
                        }
                        if (sortColumns[i].sortType != currentColumns[i].sortType) {
                            swapIndex = true;
                        }
                    }
                    if (swapIndex) {
                        topTableFilter.setIndex(index);
                        sortUsingIndex = true;
                    }
                }
            }
        }
        // 3. group by
        if (groupIndex != null) {
            Index index = getGroupSortedIndex();
            if (index != null) {
                Index current = topTableFilter.getIndex();
                if (current.getIndexType().isScan() || current == index) {
                    topTableFilter.setIndex(index);
                    isGroupSortedQuery = true;
                }
            }
        }
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
        isPrepared = true;

        return this;
    }

    private void optimizeDistinct() {
        // 1.1. distinct 单字段
        if (expressions.size() == 1) {
            Expression expr = expressions.get(0);
            expr = expr.getNonAliasExpression();
            if (expr instanceof ExpressionColumn) {
                Column column = ((ExpressionColumn) expr).getColumn();
                int selectivity = column.getSelectivity();
                Index columnIndex = topTableFilter.getTable().getIndexForColumn(column);
                if (columnIndex != null && selectivity != Constants.SELECTIVITY_DEFAULT && selectivity < 20) {
                    // the first column must be ascending
                    boolean ascending = columnIndex.getIndexColumns()[0].sortType == SortOrder.ASCENDING;
                    Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (columnIndex.supportsDistinctQuery() && ascending
                            && (current == null || current.getIndexType().isScan() || columnIndex == current)) {
                        IndexType type = columnIndex.getIndexType();
                        // hash indexes don't work, and unique single column
                        // indexes don't work
                        if (!type.isHash() && (!type.isUnique() || columnIndex.getColumns().length > 1)) {
                            topTableFilter.setIndex(columnIndex);
                            isDistinctQuery = true;
                        }
                    }
                }
            }
        }
        // 1.2. distinct 多字段
        else {
            Index current = topTableFilter.getIndex();
            if (current == null || current.getIndexType().isScan()) {
                boolean isExpressionColumn = true;
                int size = expressions.size();
                Column[] columns = new Column[size];
                for (int i = 0; isExpressionColumn && i < size; i++) {
                    Expression expr = expressions.get(i);
                    expr = expr.getNonAliasExpression();
                    isExpressionColumn &= (expr instanceof ExpressionColumn);
                    if (isExpressionColumn)
                        columns[i] = ((ExpressionColumn) expr).getColumn();
                }
                if (isExpressionColumn) {
                    for (Index index : topTableFilter.getTable().getIndexes()) {
                        IndexType type = index.getIndexType();
                        // hash indexes don't work, and unique single column
                        // indexes don't work
                        if (index.supportsDistinctQuery() && !type.isHash() && !type.isUnique()) {
                            Column[] indexColumns = index.getColumns();
                            if (indexColumns.length == size) {
                                boolean found = true;
                                for (int i = 0; found && i < size; i++) {
                                    found &= (indexColumns[i] == columns[i])
                                            && index.getIndexColumns()[i].sortType == SortOrder.ASCENDING;
                                }
                                if (found) {
                                    topTableFilter.setIndex(index);
                                    isDistinctQueryForMultiFields = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private double preparePlan() {
        TableFilter[] topArray = topFilters.toArray(new TableFilter[topFilters.size()]);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();

        setEvaluatableRecursive(topTableFilter);

        topTableFilter.prepare();
        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    if (session.getDatabase().getSettings().nestedJoins) {
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                            f.removeJoinCondition();
                            addCondition(on);
                        }
                    } else {
                        if (f.isJoinOuter()) {
                            // this will check if all columns exist - it may or
                            // may not throw an exception
                            on = on.optimize(session);
                            // it is not supported even if the columns exist
                            throw DbException.get(ErrorCode.UNSUPPORTED_OUTER_JOIN_CONDITION_1, on.getSQL());
                        }
                        f.removeJoinCondition();
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        addCondition(on);
                    }
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
        }
    }

    /**
     * Get the index that matches the ORDER BY list, if one exists. This is to
     * avoid running a separate ORDER BY if an index can be used. This is
     * specially important for large result sets, if only the first few rows are
     * important (LIMIT is used)
     *
     * @return the index if one is found
     */
    private Index getSortIndex() {
        if (sort == null) {
            return null;
        }
        ArrayList<Column> sortColumns = New.arrayList();
        for (int idx : sort.getQueryColumnIndexes()) {
            if (idx < 0 || idx >= expressions.size()) {
                throw DbException.getInvalidValueException("ORDER BY", idx + 1);
            }
            Expression expr = expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                return null;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) {
                return null;
            }
            sortColumns.add(exprCol.getColumn());
        }
        Column[] sortCols = sortColumns.toArray(new Column[sortColumns.size()]);
        int[] sortTypes = sort.getSortTypes();
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        ArrayList<Index> list = topTableFilter.getTable().getIndexes();
        if (list != null) {
            for (int i = 0, size = list.size(); i < size; i++) {
                Index index = list.get(i);
                if (index.getCreateSQL() == null) {
                    // can't use the scan index
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    continue;
                }
                IndexColumn[] indexCols = index.getIndexColumns();
                if (indexCols.length < sortCols.length) {
                    continue;
                }
                boolean ok = true;
                for (int j = 0; j < sortCols.length; j++) {
                    // the index and the sort order must start
                    // with the exact same columns
                    IndexColumn idxCol = indexCols[j];
                    Column sortCol = sortCols[j];
                    if (idxCol.column != sortCol) {
                        ok = false;
                        break;
                    }
                    if (idxCol.sortType != sortTypes[j]) {
                        // NULL FIRST for ascending and NULLS LAST
                        // for descending would actually match the default
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    return index;
                }
            }
        }
        if (sortCols.length == 1 && sortCols[0].getColumnId() == -1) {
            // special case: order by _ROWID_
            Index index = topTableFilter.getTable().getScanIndex(session);
            if (index.isRowIdIndex()) {
                return index;
            }
        }
        return null;
    }

    private Index getGroupSortedIndex() {
        ArrayList<Index> indexes = topTableFilter.getTable().getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index.getIndexType().isScan()) {
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    // does not allow scanning entries
                    continue;
                }
                if (isGroupSortedIndex(topTableFilter, index)) {
                    return index;
                }
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(TableFilter tableFilter, Index index) {
        // check that all the GROUP BY expressions are part of the index
        Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        boolean[] grouped = new boolean[indexColumns.length];
        outerLoop: for (int i = 0, size = expressions.size(); i < size; i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (tableFilter == exprCol.getTableFilter()) {
                    if (indexColumns[j].equals(exprCol.getColumn())) {
                        grouped[j] = true;
                        continue outerLoop;
                    }
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected LocalResult queryWithoutCache(int maxRows, ResultTarget target) {
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        int columnCount = expressions.size();
        LocalResult result = null;
        if (target == null || !session.getDatabase().getSettings().optimizeInsertFromSelect) {
            result = createLocalResult(result);
        }
        if (sort != null && (!sortUsingIndex || distinct)) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
        }
        if (distinct && (!isDistinctQuery && !isDistinctQueryForMultiFields)) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result = createLocalResult(result);
            // result.setRandomAccess(); //见H2的Mainly MVStore improvements的提交记录
        }
        if (isGroupQuery && !isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (limitRows >= 0 || offsetExpr != null) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        boolean exclusive = isForUpdate && !isForUpdateMvcc;
        if (isForUpdateMvcc) {
            if (isGroupQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && GROUP");
            } else if (distinct) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && DISTINCT");
            } else if (isQuickAggregateQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && AGGREGATE");
            } else if (topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && JOIN");
            }
        }
        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if (isQuickAggregateQuery) {
                queryQuick(columnCount, to);
            } else if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    queryGroupSorted(columnCount, to);
                } else {
                    queryGroup(columnCount, result);
                }
            } else if (isDistinctQuery) {
                queryDistinct(to, limitRows);
            } else if (isDistinctQueryForMultiFields) {
                queryDistinctForMultiFields(to, limitRows);
            } else {
                queryFlat(columnCount, to, limitRows);
            }
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        if (limitRows >= 0) {
            result.setLimit(limitRows);
        }
        if (result != null) {
            result.done();
            if (target != null) {
                while (result.next()) {
                    target.addRow(result.currentRow());
                }
                result.close();
                return null;
            }
            return result;
        }
        return null;
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray, visibleColumnCount);
    }

    private void queryDistinctForMultiFields(ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        int[] columnIds = index.getColumnIds();
        int size = columnIds.length;
        int sampleSize = getSampleSizeValue(session);
        Cursor cursor = index.findDistinct(session, null, null);
        while (cursor.next()) {
            setCurrentRowNumber(rowNumber + 1);
            SearchRow found = cursor.getSearchRow();
            Value[] row = new Value[size];
            for (int i = 0; i < size; i++) {
                row[i] = found.getValue(columnIds[i]);
            }
            result.addRow(row);
            rowNumber++;
            if ((sort == null || sortUsingIndex) && limitRows > 0 && rowNumber >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    // 单字段distinct
    private void queryDistinct(ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        int columnIndex = index.getColumns()[0].getColumnId();
        int sampleSize = getSampleSizeValue(session);
        Cursor cursor = index.findDistinct(session, null, null);
        while (cursor.next()) {
            setCurrentRowNumber(rowNumber + 1);
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            Value[] row = { value };
            result.addRow(row);
            rowNumber++;
            if ((sort == null || sortUsingIndex) && limitRows > 0 && rowNumber >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    private void queryFlat(int columnCount, ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ArrayList<Row> forUpdateRows = null;
        if (isForUpdateMvcc) {
            forUpdateRows = New.arrayList();
        }
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                if (isForUpdateMvcc) {
                    topTableFilter.lockRowAdd(forUpdateRows);
                }
                result.addRow(row);
                rowNumber++;
                if ((sort == null || sortUsingIndex) && limitRows > 0 && result.getRowCount() >= limitRows) {
                    break;
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (isForUpdateMvcc) {
            topTableFilter.lockRows(forUpdateRows);
        }
    }

    private void queryQuick(int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }

    private void queryGroupSorted(int columnCount, ResultTarget result) {
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        Value[] previousKeyValues = null;
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                rowNumber++;
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                }
                currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (!groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
    }

    private void addGroupSortedRow(Value[] keyValues, int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int j = 0; j < groupIndex.length; j++) {
            row[groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (groupByExpression[j]) {
                continue;
            }
            Expression expr = expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (isHavingNullOrFalse(row)) {
            return;
        }
        row = keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        if (havingIndex >= 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) {
                return true;
            }
            if (!Boolean.TRUE.equals(v.getBoolean())) {
                return true;
            }
        }
        return false;
    }

    private Value[] keepOnlyDistinct(Value[] row, int columnCount) {
        if (columnCount == distinctColumnCount) {
            return row;
        }
        // remove columns so that 'distinct' can filter duplicate rows
        Value[] r2 = new Value[distinctColumnCount];
        System.arraycopy(row, 0, r2, 0, distinctColumnCount);
        return r2;
    }

    // 除了QuickAggregateQuery和GroupSortedQuery外，其他场景的聚合函数、group by、having都在这里处理
    // groupIndex和groupByExpression为null的时候，表示没有group by
    private void queryGroup(int columnCount, LocalResult result) {
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                } else {
                    // 避免在ExpressionColumn.getValue中取到旧值
                    // 例如SELECT id/3 AS A, COUNT(*) FROM mytable GROUP BY A HAVING A>=0
                    currentGroup = null;
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                currentGroup = values;
                currentGroupRowId++;
                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }

    public Result queryGroupMerge() {
        int columnCount = expressions.size();
        LocalResult result = new LocalResult(session, expressionArray, columnCount);
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        topTableFilter.reset();
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            Value key;
            rowNumber++;
            if (groupIndex == null) {
                key = defaultGroup;
            } else {
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    keyValues[i] = topTableFilter.getValue(idx);
                }
                key = ValueArray.get(keyValues);
            }
            HashMap<Expression, Object> values = groups.get(key);
            if (values == null) {
                values = new HashMap<Expression, Object>();
                groups.put(key, values);
            }
            currentGroup = values;
            currentGroupRowId++;
            for (int i = 0; i < columnCount; i++) {
                if (groupByExpression == null || !groupByExpression[i]) {
                    Expression expr = expressions.get(i);
                    expr.mergeAggregate(session, topTableFilter.getValue(i));
                }
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getMergedValue(session);
            }
            result.addRow(row);
        }

        return result;
    }

    public Result calculate(Result result, Select select) {
        int size = expressions.size();
        if (havingIndex >= 0)
            size--;
        if (size == select.expressions.size())
            return result;

        int columnCount = visibleColumnCount;
        LocalResult lr = new LocalResult(session, expressionArray, columnCount);

        Calculator calculator;
        int index = 0;
        while (result.next()) {
            calculator = new Calculator(result.currentRow());
            for (int i = 0; i < columnCount; i++) {
                Expression expr = expressions.get(i);
                index = calculator.getIndex();
                expr.calculate(calculator);
                if (calculator.getIndex() == index) {
                    calculator.addResultValue(calculator.getValue(index));
                    calculator.addIndex();
                }
            }

            lr.addRow(calculator.getResult().toArray(new Value[0]));
        }

        return lr;
    }

    @Override
    public Result getMetaData() {
        LocalResult result = new LocalResult(session, expressionArray, visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = New.hashSet();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    @Override
    public void fireBeforeSelectTriggers() {
        for (int i = 0, size = filters.size(); i < size; i++) {
            TableFilter filter = filters.get(i);
            filter.getTable().fire(session, Trigger.SELECT, true);
        }
    }

    @Override
    public String getPlanSQL() {
        return getPlanSQL(false, false);
    }

    @Override
    public String getPlanSQL(boolean isDistributed) {
        if (isGroupQuery() || getLimit() != null || getOffset() != null)
            return getPlanSQL(isDistributed, false);
        else
            return getSQL();
    }

    public String getPlanSQL(boolean isDistributed, boolean isMerged) {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (distinct) {
            buff.append(" DISTINCT");
        }

        int columnCount = visibleColumnCount;
        if (isDistributed)
            columnCount = expressions.size();
        for (int i = 0; i < columnCount; i++) {
            if (isDistributed && havingIndex >= 0 && i == havingIndex)
                continue;
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(exprList[i].getSQL(isDistributed), 4, false));
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst("\n");
                buff.append(filter.getPlanSQL(i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : topFilters) {
                do {
                    buff.appendExceptFirst("\n");
                    buff.append(f.getPlanSQL(i++ > 0));
                    f = f.getJoin();
                } while (f != null);
            }
        }
        // 合并时可以忽略WHERE子句
        if (!isMerged) {
            if (condition != null) {
                buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
            }
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL(isDistributed)));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL(isDistributed)));
            }
        }

        // 合并时可以忽略HAVING、ORDER BY等等子句
        if (isMerged)
            return buff.toString();

        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL(isDistributed)));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL(isDistributed)));
        }
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (orderList != null) {
            buff.append("\nORDER BY ");
            buff.resetCount();
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
        }
        if (limitExpr != null) {
            if (isDistributed) {
                int limit = limitExpr.getValue(session).getInt();
                if (offsetExpr != null)
                    limit += offsetExpr.getValue(session).getInt();

                buff.append("\nLIMIT ").append(limit);
            } else {
                buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL(isDistributed)));
                if (offsetExpr != null) {
                    buff.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL(isDistributed)));
                }
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        if (isQuickAggregateQuery) {
            buff.append("\n/* direct lookup */");
        }
        if (isDistinctQuery) {
            buff.append("\n/* distinct */");
        }
        if (sortUsingIndex) {
            buff.append("\n/* index sorted */");
        }
        if (isGroupQuery) {
            if (isGroupSortedQuery) {
                buff.append("\n/* group sorted */");
            }
        }
        // buff.append("\n/* cost: " + cost + " */");
        return buff.toString();
    }

    @Override
    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
        if (session.getDatabase().getSettings().selectForUpdateMvcc && session.getDatabase().isMultiVersion()) {
            isForUpdateMvcc = b;
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    /**
     * Check if this is an aggregate query with direct lookup, for example a
     * query of the type SELECT COUNT(*) FROM TEST or
     * SELECT MAX(ID) FROM TEST.
     *
     * @return true if a direct lookup is possible
     */
    public boolean isQuickAggregateQuery() {
        return isQuickAggregateQuery;
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(session, comparisonType, col, param);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(session, Comparison.EQUAL_NULL_SAFE, param, param);
        }
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                } else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    @Override
    public void updateAggregate(ServerSession s) {
        for (Expression e : expressions) {
            e.updateAggregate(s);
        }
        if (condition != null) {
            condition.updateAggregate(s);
        }
        if (having != null) {
            having.updateAggregate(s);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC: {
            if (isForUpdate) {
                return false;
            }
            for (int i = 0, size = filters.size(); i < size; i++) {
                TableFilter f = filters.get(i);
                if (!f.getTable().isDeterministic()) {
                    return false;
                }
            }
            break;
        }
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
            for (int i = 0, size = filters.size(); i < size; i++) {
                TableFilter f = filters.get(i);
                long m = f.getTable().getMaxDataModificationId();
                visitor.addDataModificationId(m);
            }
            break;
        }
        case ExpressionVisitor.EVALUATABLE: {
            if (!session.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                return false;
            }
            break;
        }
        case ExpressionVisitor.GET_DEPENDENCIES: {
            for (int i = 0, size = filters.size(); i < size; i++) {
                TableFilter f = filters.get(i);
                Table table = f.getTable();
                visitor.addDependency(table);
                table.addDependencies(visitor.getDependencies());
            }
            break;
        }
        default:
        }
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        boolean result = true;
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i);
            if (!e.isEverything(v2)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(v2)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(v2)) {
            result = false;
        }
        return result;
    }

    @Override
    public boolean allowGlobalConditions() {
        if (offsetExpr == null && (limitExpr == null || sort == null)) {
            return true;
        }
        return false;
    }

    public SortOrder getSortOrder() {
        return sort;
    }

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
            throw DbException.throwInternalError("indexConditionType: " + indexConditionType);
        }
        this.addGlobalCondition((Parameter) param, columnId, comparisonType);
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 127)
            return priority;

        priority = MIN_PRIORITY;
        return priority;
    }

    @Override
    public TableFilter getTableFilter() {
        return topTableFilter;
    }
}
