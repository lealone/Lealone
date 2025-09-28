/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StatementBuilder;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.api.Trigger;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.async.Future;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.result.ResultTarget;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.SelectOrderBy;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.condition.ConditionAndOr;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.expression.visitor.ExpressionVisitorFactory;
import com.lealone.sql.optimizer.ColumnResolver;
import com.lealone.sql.optimizer.Optimizer;
import com.lealone.sql.optimizer.PlanItem;
import com.lealone.sql.optimizer.TableFilter;

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
 * @author H2 Group
 * @author zhh
 */
public class Select extends Query {

    private TableFilter topTableFilter;
    private final ArrayList<TableFilter> filters = Utils.newSmallArrayList();
    private final ArrayList<TableFilter> topFilters = Utils.newSmallArrayList();
    private ArrayList<Expression> group;
    private Expression having;
    int[] groupIndex;
    boolean[] groupByExpression;
    int havingIndex;
    HashMap<Expression, Object> currentGroup;
    int currentGroupRowId;
    Expression condition;
    int visibleColumnCount;
    int resultColumnCount; // 不包含having和group by中加入的列
    boolean isGroupQuery;
    boolean isGroupSortedQuery;
    boolean isQuickAggregateQuery;
    boolean isDistinctQuery;
    boolean sortUsingIndex;
    private double cost;

    final QueryResultCache resultCache = new QueryResultCache(this);

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

    public int getResultColumnCount() {
        return resultColumnCount;
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

    public boolean isGroupSortedQuery() {
        return isGroupSortedQuery;
    }

    public boolean isDistinctQuery() {
        return isDistinctQuery;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public void setCurrentGroup(HashMap<Expression, Object> currentGroup) {
        this.currentGroup = currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    public void incrementCurrentGroupRowId() {
        currentGroupRowId++;
    }

    public int[] getGroupIndex() {
        return groupIndex;
    }

    public boolean[] getGroupByExpression() {
        return groupByExpression;
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
    public ArrayList<TableFilter> getFilters() {
        return filters;
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
            expressionSQL = new ArrayList<>(visibleColumnCount);
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
            initOrder(session, expressions, expressionSQL, orderList, visibleColumnCount, distinct,
                    filters);
            // prepare阶段还用到orderList，所以先不置null
        }
        resultColumnCount = expressions.size();

        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }
        if (group != null) {
            initGroup(expressionSQL);
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

    // 为groupIndex和groupByExpression两个字段赋值，
    // groupIndex记录了GROUP BY子句中的字段在select字段列表中的位置索引(从0开始计数)
    // groupByExpression数组的大小跟select字段列表一样，类似于一个bitmap，用来记录select字段列表中的哪些字段是GROUP BY字段
    // 如果GROUP BY子句中的字段不在select字段列表中，那么会把它加到select字段列表
    private void initGroup(ArrayList<String> expressionSQL) {
        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
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
        if (orderList != null) {
            sort = prepareOrder(session, orderList, expressions.size());
            orderList = null;
        }
        rawExpressionInfoList = new ArrayList<>(expressions.size());
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            String[] eInfo = { e.getAlias(), e.getColumnName() };
            rawExpressionInfoList.add(eInfo);
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
        if (condition == null && isGroupQuery && groupIndex == null && havingIndex < 0
                && filters.size() == 1) {
            Table t = filters.get(0).getTable();
            isQuickAggregateQuery = accept(ExpressionVisitorFactory.getOptimizableVisitor(t));
        }

        cost = preparePlan(); // 选择合适的索引

        // 以下3个if为特殊的distinct、sort、group by选择更合适的索引
        // 1. distinct
        if (distinct && session.getDatabase().getSettings().optimizeDistinct && !isGroupQuery
                && filters.size() == 1 && condition == null) {
            optimizeDistinct();
        }
        // 2. sort
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            optimizeSort();
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
                if (columnIndex != null && selectivity != Constants.SELECTIVITY_DEFAULT
                        && selectivity < 20) {
                    // the first column must be ascending
                    boolean ascending = columnIndex.getIndexColumns()[0].sortType == SortOrder.ASCENDING;
                    Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (columnIndex.supportsDistinctQuery() && ascending && (current == null
                            || current.getIndexType().isScan() || columnIndex == current)) {
                        IndexType type = columnIndex.getIndexType();
                        // hash indexes don't work, and unique single column
                        // indexes don't work
                        if (!type.isHash()
                                && (!type.isUnique() || columnIndex.getColumns().length > 1)) {
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
                                    found &= (indexColumns[i] == columns[i]) && index
                                            .getIndexColumns()[i].sortType == SortOrder.ASCENDING;
                                }
                                if (found) {
                                    topTableFilter.setIndex(index);
                                    isDistinctQuery = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void optimizeSort() {
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

    private double preparePlan() {
        // 优化单表查询
        if (filters.size() == 1) {
            topTableFilter = filters.get(0);
            setEvaluatableRecursive(topTableFilter);
            PlanItem item = topTableFilter.preparePlan(session, 1);
            return item.getCost();
        }

        TableFilter[] topArray = topFilters.toArray(new TableFilter[0]);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }
        Optimizer optimizer = new Optimizer(topArray, session);
        topTableFilter = optimizer.optimize();
        setEvaluatableRecursive(topTableFilter);
        topTableFilter.prepare();
        return optimizer.getCost();
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null && !on.isEvaluatable()) {
                // need to check that all added are bound to a table
                on = on.optimize(session);
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    f.removeJoinCondition();
                    addCondition(on);
                }
            }
            on = f.getFilterCondition();
            if (on != null && !on.isEvaluatable()) {
                f.removeFilterCondition();
                addCondition(on);
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
        ArrayList<Column> sortColumns = new ArrayList<>();
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
    public Future<Result> getMetaData() {
        LocalResult result = new LocalResult(session, expressionArray, visibleColumnCount,
                rawExpressionInfoList);
        result.done();
        return Future.succeededFuture(result);
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = new HashSet<>(filters.size());
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
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (distinct) {
            buff.append(" DISTINCT");
        }

        int columnCount = visibleColumnCount;
        for (int i = 0; i < columnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(exprList[i].getSQL(), 4, false));
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
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }

        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
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
            buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
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
    public boolean allowGlobalConditions() {
        if (offsetExpr == null && (limitExpr == null || sort == null)) {
            return true;
        }
        return false;
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.accept(ExpressionVisitorFactory.getQueryComparableVisitor())) {
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
    public boolean isDeterministic() {
        if (isForUpdate) {
            return false;
        }
        for (int i = 0, size = filters.size(); i < size; i++) {
            TableFilter f = filters.get(i);
            if (!f.getTable().isDeterministic()) {
                return false;
            }
        }
        return true;
    }

    public boolean isEvaluatable() {
        if (!session.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
            return false;
        }
        return true;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitSelect(this);
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 127)
            return priority;

        priority = MIN_PRIORITY;
        return priority;
    }

    public TableFilter getTableFilter() {
        return topTableFilter;
    }

    public HashSet<Column> getReferencedColumns() {
        int len = expressionArray.length;
        HashSet<Column> columnSet = new HashSet<>(len);
        for (int i = 0; i < len; i++) {
            expressionArray[i].getColumns(columnSet);
        }
        if (condition != null)
            condition.getColumns(columnSet);

        return columnSet;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public void disableCache() {
        resultCache.disable();
    }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncResultHandler<Result> asyncHandler, ResultTarget target) {
        return new YieldableSelect(this, maxRows, scrollable, asyncHandler, target);
    }
}
