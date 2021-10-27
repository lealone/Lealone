/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.SysProperties;
import org.lealone.db.auth.Right;
import org.lealone.db.index.Index;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.IExpression;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.query.Select;
import org.lealone.storage.PageKey;

/**
 * A table filter represents a table that is used in a query. There is one such
 * object whenever a table (or view) is used in a query. For example the
 * following query has 2 table filters: SELECT * FROM TEST T1, TEST T2.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TableFilter extends ColumnResolverBase {

    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2, NULL_ROW = 3;

    /**
     * Whether this is a direct or indirect (nested) outer join
     */
    private boolean joinOuterIndirect;

    private ServerSession session;

    private final Table table;
    private final Select select;
    private String alias;
    private Index index;
    private int scanCount;
    private boolean evaluatable;

    /**
     * Indicates that this filter is used in the plan.
     */
    private boolean used;

    /**
     * The filter used to walk through the index.
     */
    private final IndexCursor cursor;

    /**
     * The index conditions used for direct index lookup (start or end).
     */
    private final ArrayList<IndexCondition> indexConditions = Utils.newSmallArrayList();

    /**
     * Additional conditions that can't be used for index lookup, but for row
     * filter for this table (ID=ID, NAME LIKE '%X%')
     */
    private Expression filterCondition;

    /**
     * The complete join condition.
     */
    private Expression joinCondition;

    private SearchRow currentSearchRow;
    private Row current;
    private int state;

    /**
     * The joined table (if there is one).
     */
    private TableFilter join;

    /**
     * Whether this is an outer join.
     */
    private boolean joinOuter;

    /**
     * The nested joined table (if there is one).
     */
    private TableFilter nestedJoin;

    private ArrayList<Column> naturalJoinColumns;
    private boolean foundOne;
    private Expression fullCondition;
    private final int hashCode;

    private int[] columnIndexes;

    /**
     * Create a new table filter object.
     *
     * @param session the session
     * @param table the table from where to read data
     * @param alias the alias name
     * @param rightsChecked true if rights are already checked
     * @param select the select statement
     */
    public TableFilter(ServerSession session, Table table, String alias, boolean rightsChecked, Select select) {
        this.session = session;
        this.table = table;
        this.alias = alias;
        this.select = select;
        this.cursor = new IndexCursor(this);
        if (!rightsChecked) {
            session.getUser().checkRight(table, Right.SELECT);
        }
        hashCode = session.nextObjectId();
    }

    @Override
    public Select getSelect() {
        return select;
    }

    public Table getTable() {
        return table;
    }

    public IndexCursor getCursor() {
        return cursor;
    }

    /**
     * Lock the table. This will also lock joined tables.
     *
     * @param s the session
     * @param exclusive true if an exclusive lock is required
     */
    public void lock(ServerSession s, boolean exclusive) {
        table.lock(s, exclusive);
        if (join != null) {
            join.lock(s, exclusive);
        }
    }

    /**
     * Get the best plan item (index, cost) to use for the current join order.
     *
     * @param s the session
     * @param level 1 for the first table in a join, 2 for the second, and so on
     * @return the best plan item
     */
    public PlanItem getBestPlanItem(ServerSession s, int level) {
        PlanItem item;
        if (indexConditions.isEmpty()) {
            item = new PlanItem();
            item.setIndex(table.getScanIndex(s));
            item.cost = item.getIndex().getCost(s, null, null);
        } else {
            int len = table.getColumns().length;
            int[] masks = new int[len];
            for (IndexCondition condition : indexConditions) {
                if (condition.isEvaluatable()) {
                    if (condition.isAlwaysFalse()) {
                        masks = null;
                        break;
                    }
                    int id = condition.getColumn().getColumnId();
                    if (id >= 0) {
                        // 多个IndexCondition可能是同一个字段
                        // 如id>1 and id <10，这样masks[id]最后就变成IndexCondition.RANGE了
                        masks[id] |= condition.getMask(indexConditions);
                    }
                }
            }
            SortOrder sortOrder = null;
            if (select != null) {
                sortOrder = select.getSortOrder();
            }
            item = Optimizer.getBestPlanItem(s, masks, table, sortOrder);
            // The more index conditions, the earlier the table.
            // This is to ensure joins without indexes run quickly:
            // x (x.a=10); y (x.b=y.b) - see issue 113
            item.cost -= item.cost * indexConditions.size() / 100 / level;
        }
        if (nestedJoin != null) {
            setEvaluatable(true);
            item.setNestedJoinPlan(nestedJoin.getBestPlanItem(s, level));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getNestedJoinPlan().cost;
        }
        if (join != null) {
            setEvaluatable(true);
            item.setJoinPlan(join.getBestPlanItem(s, level));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getJoinPlan().cost;
        }
        return item;
    }

    /**
     * Set what plan item (index, cost) to use use.
     *
     * @param item the plan item
     */
    public void setPlanItem(PlanItem item) {
        if (item == null) {
            // invalid plan, most likely because a column wasn't found
            // this will result in an exception later on
            return;
        }
        setIndex(item.getIndex());
        if (nestedJoin != null && item.getNestedJoinPlan() != null) {
            nestedJoin.setPlanItem(item.getNestedJoinPlan());
        }
        if (join != null && item.getJoinPlan() != null) {
            join.setPlanItem(item.getJoinPlan());
        }
    }

    /**
     * Prepare reading rows. This method will remove all index conditions that
     * can not be used, and optimize the conditions.
     */
    public void prepare() {
        // forget all unused index conditions
        // the indexConditions list may be modified here
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition condition = indexConditions.get(i);
            if (!condition.isAlwaysFalse()) {
                Column col = condition.getColumn();
                if (col.getColumnId() >= 0) {
                    if (index.getColumnIndex(col) < 0) {
                        indexConditions.remove(i);
                        i--;
                    }
                }
            }
        }
        if (nestedJoin != null) {
            if (SysProperties.CHECK && nestedJoin == this) {
                DbException.throwInternalError("self join");
            }
            nestedJoin.prepare();
        }
        if (join != null) {
            if (SysProperties.CHECK && join == this) {
                DbException.throwInternalError("self join");
            }
            join.prepare();
        }
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        if (joinCondition != null) {
            joinCondition = joinCondition.optimize(session);
        }
    }

    // 为单表update/delete/select寻找一个执行计划
    public PlanItem preparePlan(ServerSession s, int level) {
        PlanItem item = getBestPlanItem(s, level);
        setPlanItem(item);
        prepare();
        return item;
    }

    /**
     * Start the query. This will reset the scan counts.
     *
     * @param s the session
     */
    public void startQuery(ServerSession s) {
        this.session = s;
        scanCount = 0;
        if (nestedJoin != null) {
            nestedJoin.startQuery(s);
        }
        if (join != null) {
            join.startQuery(s);
        }
    }

    /**
     * Reset to the current position.
     */
    public void reset() {
        if (nestedJoin != null) {
            nestedJoin.reset();
        }
        if (join != null) {
            join.reset();
        }
        state = BEFORE_FIRST;
        foundOne = false;
    }

    /**
     * Check if there are more rows to read.
     *
     * @return true if there are
     */
    public boolean next() {
        if (state == AFTER_LAST) {
            return false;
        } else if (state == BEFORE_FIRST) {
            cursor.find(session, indexConditions);
            if (!cursor.isAlwaysFalse()) {
                if (nestedJoin != null) {
                    nestedJoin.reset();
                }
                if (join != null) {
                    join.reset();
                }
            }
        } else {
            // state == FOUND || NULL_ROW
            // the last row was ok - try next row of the join
            if (join != null && join.next()) {
                return true;
            }
        }
        while (true) {
            // go to the next row
            if (state == NULL_ROW) {
                break;
            }
            if (cursor.isAlwaysFalse()) {
                state = AFTER_LAST;
            } else if (nestedJoin != null) {
                if (state == BEFORE_FIRST) {
                    state = FOUND;
                }
            } else {
                if ((++scanCount & 4095) == 0) {
                    checkTimeout();
                }
                if (cursor.next()) {
                    currentSearchRow = cursor.getSearchRow();
                    current = null;
                    state = FOUND;
                } else {
                    state = AFTER_LAST;
                }
            }
            if (nestedJoin != null && state == FOUND) {
                if (!nestedJoin.next()) {
                    state = AFTER_LAST;
                    if (joinOuter && !foundOne) {
                        // possibly null row
                    } else {
                        continue;
                    }
                }
            }
            // if no more rows found, try the null row (for outer joins only)
            if (state == AFTER_LAST) {
                if (joinOuter && !foundOne) {
                    setNullRow();
                } else {
                    break;
                }
            }
            if (!isOk(filterCondition)) {
                continue;
            }
            boolean joinConditionOk = isOk(joinCondition);
            if (state == FOUND) {
                if (joinConditionOk) {
                    foundOne = true;
                } else {
                    continue;
                }
            }
            if (join != null) {
                join.reset();
                if (!join.next()) {
                    continue;
                }
            }
            // check if it's ok
            if (state == NULL_ROW || joinConditionOk) {
                return true;
            }
        }
        state = AFTER_LAST;
        return false;
    }

    /**
     * Set the state of this and all nested tables to the NULL row.
     */
    private void setNullRow() {
        state = NULL_ROW;
        current = table.getNullRow();
        currentSearchRow = current;
        if (nestedJoin != null) {
            nestedJoin.visit(f -> f.setNullRow());
        }
    }

    private void checkTimeout() {
        session.checkCanceled();
    }

    private boolean isOk(Expression condition) {
        return condition == null || condition.getBooleanValue(session);
    }

    /**
     * Get the current row.
     *
     * @return the current row, or null
     */
    public Row get() {
        if (current == null && currentSearchRow != null) {
            current = cursor.get(getColumnIndexes());
        }
        return current;
    }

    /**
     * Set the current row.
     *
     * @param current the current row
     */
    public void set(Row current) {
        this.current = current;
        this.currentSearchRow = current;
    }

    /**
     * Get the table alias name. If no alias is specified, the table name is
     * returned.
     *
     * @return the alias name
     */
    @Override
    public String getTableAlias() {
        if (alias != null) {
            return alias;
        }
        return table.getName();
    }

    /**
     * Add an index condition.
     *
     * @param condition the index condition
     */
    public void addIndexCondition(IndexCondition condition) {
        indexConditions.add(condition);
    }

    /**
     * Add a filter condition.
     *
     * @param condition the condition
     * @param isJoin if this is in fact a join condition
     */
    public void addFilterCondition(Expression condition, boolean isJoin) {
        if (isJoin) {
            joinCondition = createCondition(joinCondition, condition);
        } else {
            filterCondition = createCondition(filterCondition, condition);
        }
    }

    private Expression createCondition(Expression oldC, Expression newC) {
        return oldC == null ? newC : new ConditionAndOr(ConditionAndOr.AND, oldC, newC);
    }

    /**
     * Add a joined table.
     *
     * @param filter the joined table filter
     * @param outer if this is an outer join
     * @param on the join condition
     */
    public void addJoin(TableFilter filter, boolean outer, final Expression on) {
        if (on != null) {
            on.mapColumns(this, 0);
            TableFilterVisitor visitor = new MapColumnsVisitor(on);
            visit(visitor);
            filter.visit(visitor);
        }
        if (join == null) {
            join = filter;
            filter.joinOuter = outer;
            if (outer) {
                filter.visit(JOI_VISITOR);
            }
            if (on != null) {
                filter.mapAndAddFilter(on);
            }
        } else {
            join.addJoin(filter, outer, on);
        }
    }

    /**
     * A visitor that sets joinOuterIndirect to true.
     */
    private static final TableFilterVisitor JOI_VISITOR = f -> f.joinOuterIndirect = true;

    /**
     * A visitor that maps columns.
     */
    private static final class MapColumnsVisitor implements TableFilterVisitor {
        private final Expression on;

        MapColumnsVisitor(Expression on) {
            this.on = on;
        }

        @Override
        public void accept(TableFilter f) {
            on.mapColumns(f, 0);
        }
    }

    /**
     * Map the columns and add the join condition.
     *
     * @param on the condition
     */
    public void mapAndAddFilter(Expression on) {
        on.mapColumns(this, 0);
        addFilterCondition(on, true);
        on.createIndexConditions(session, this);
        if (nestedJoin != null) {
            on.mapColumns(nestedJoin, 0);
            on.createIndexConditions(session, nestedJoin);
        }
        if (join != null) {
            join.mapAndAddFilter(on);
        }
    }

    public TableFilter getJoin() {
        return join;
    }

    /**
     * Whether this is an outer joined table.
     *
     * @return true if it is
     */
    public boolean isJoinOuter() {
        return joinOuter;
    }

    /**
     * Whether this is indirectly an outer joined table (nested within an inner
     * join).
     *
     * @return true if it is
     */
    public boolean isJoinOuterIndirect() {
        return joinOuterIndirect;
    }

    /**
     * Get the query execution plan text to use for this table filter.
     *
     * @param isJoin if this is a joined table
     * @return the SQL statement snippet
     */
    public String getPlanSQL(boolean isJoin) {
        StringBuilder buff = new StringBuilder();
        if (isJoin) {
            if (joinOuter) {
                buff.append("LEFT OUTER JOIN ");
            } else {
                buff.append("INNER JOIN ");
            }
        }
        if (nestedJoin != null) {
            StringBuffer buffNested = new StringBuffer();
            TableFilter n = nestedJoin;
            do {
                buffNested.append(n.getPlanSQL(n != nestedJoin));
                buffNested.append('\n');
                n = n.getJoin();
            } while (n != null);
            String nested = buffNested.toString();
            boolean enclose = !nested.startsWith("(");
            if (enclose) {
                buff.append("(\n");
            }
            buff.append(StringUtils.indent(nested, 4, false));
            if (enclose) {
                buff.append(')');
            }
            if (isJoin) {
                buff.append(" ON ");
                if (joinCondition == null) {
                    // need to have a ON expression,
                    // otherwise the nesting is unclear
                    buff.append("1=1");
                } else {
                    buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
                }
            }
            return buff.toString();
        }
        buff.append(table.getSQL());
        if (alias != null) {
            buff.append(' ').append(session.getDatabase().quoteIdentifier(alias));
        }
        if (index != null) {
            buff.append('\n');
            StatementBuilder planBuff = new StatementBuilder();
            planBuff.append(index.getPlanSQL());
            if (indexConditions.size() > 0) {
                planBuff.append(": ");
                for (IndexCondition condition : indexConditions) {
                    planBuff.appendExceptFirst("\n    AND ");
                    planBuff.append(condition.getSQL());
                }
            }
            String plan = StringUtils.quoteRemarkSQL(planBuff.toString());
            if (plan.indexOf('\n') >= 0) {
                plan += "\n";
            }
            buff.append(StringUtils.indent("/* " + plan + " */", 4, false));
        }
        if (isJoin) {
            buff.append("\n    ON ");
            if (joinCondition == null) {
                // need to have a ON expression, otherwise the nesting is
                // unclear
                buff.append("1=1");
            } else {
                buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
            }
        }
        if (filterCondition != null) {
            buff.append('\n');
            String condition = StringUtils.unEnclose(filterCondition.getSQL());
            condition = "/* WHERE " + StringUtils.quoteRemarkSQL(condition) + "\n*/";
            buff.append(StringUtils.indent(condition, 4, false));
        }
        if (scanCount > 0) {
            buff.append("\n    /* scanCount: ").append(scanCount).append(" */");
        }
        return buff.toString();
    }

    /**
     * Remove all index conditions that are not used by the current index.
     */
    void removeUnusableIndexConditions() {
        // the indexConditions list may be modified here
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition cond = indexConditions.get(i);
            if (!cond.isEvaluatable()) {
                indexConditions.remove(i--);
            }
        }
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
        cursor.setIndex(index);
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public boolean isUsed() {
        return used;
    }

    /**
     * Set the session of this table filter.
     *
     * @param session the new session
     */
    void setSession(ServerSession session) {
        this.session = session;
    }

    /**
     * Remove the joined table
     */
    public void removeJoin() {
        this.join = null;
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    /**
     * Remove the join condition.
     */
    public void removeJoinCondition() {
        this.joinCondition = null;
    }

    public Expression getFilterCondition() {
        return filterCondition;
    }

    /**
     * Remove the filter condition.
     */
    public void removeFilterCondition() {
        this.filterCondition = null;
    }

    public void setFullCondition(Expression condition) {
        this.fullCondition = condition;
        if (join != null) {
            join.setFullCondition(condition);
        }
    }

    /**
     * Optimize the full condition. This will add the full condition to the
     * filter condition.
     */
    void optimizeFullCondition() {
        if (fullCondition != null) {
            fullCondition.addFilterConditions(this, joinOuter);
            if (nestedJoin != null) {
                nestedJoin.optimizeFullCondition();
            }
            if (join != null) {
                join.optimizeFullCondition();
            }
        }
    }

    /**
     * Update the filter and join conditions of this and all joined tables with
     * the information that the given table filter and all nested filter can now
     * return rows or not.
     *
     * @param filter the table filter
     * @param b the new flag
     */
    public void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(b);
        if (nestedJoin != null) {
            // don't enable / disable the nested join filters
            // if enabling a filter in a joined filter
            if (this == filter) {
                nestedJoin.setEvaluatable(nestedJoin, b);
            }
        }
        if (join != null) {
            join.setEvaluatable(filter, b);
        }
    }

    public void setEvaluatable(boolean evaluatable) {
        this.evaluatable = evaluatable;
    }

    public boolean isEvaluatable() {
        return evaluatable;
    }

    @Override
    public String getSchemaName() {
        return table.getSchema().getName();
    }

    @Override
    public Column[] getColumns() {
        return table.getColumns();
    }

    /**
     * Get the system columns that this table understands. This is used for
     * compatibility with other databases. The columns are only returned if the
     * current mode supports system columns.
     *
     * @return the system columns
     */
    @Override
    public Column[] getSystemColumns() {
        if (!session.getDatabase().getMode().systemColumns) {
            return null;
        }
        Column[] sys = new Column[3];
        sys[0] = new Column("oid", Value.INT);
        sys[0].setTable(table, 0);
        sys[1] = new Column("ctid", Value.STRING);
        sys[1].setTable(table, 0);
        sys[2] = new Column("CTID", Value.STRING);
        sys[2].setTable(table, 0);
        return sys;
    }

    @Override
    public Column getRowIdColumn() {
        if (session.getDatabase().getSettings().rowId) {
            return table.getRowIdColumn();
        }
        return null;
    }

    public int getCurrentSearchRowLength() {
        return currentSearchRow.getColumnCount();
    }

    public Row rebuildSearchRow(ServerSession session, Row oldRow) {
        Row newRow = table.getRow(session, oldRow.getKey(), oldRow.getRawValue());
        current = newRow;
        currentSearchRow = newRow;
        return newRow;
    }

    @Override
    public Value getValue(Column column) {
        return getValue(column.getColumnId());
    }

    private int batchSize;

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Value getValue(int columnId) {
        if (currentSearchRow == null) {
            return null;
        }

        if (columnId == -1) {
            return ValueLong.get(currentSearchRow.getKey());
        }
        if (current == null) {
            Value v = currentSearchRow.getValue(columnId);
            if (v != null) {
                return v;
            }
            current = cursor.get();
            if (current == null) {
                return ValueNull.INSTANCE;
            }
        }
        return current.getValue(columnId);
    }

    @Override
    public TableFilter getTableFilter() {
        return this;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return alias != null ? alias : table.toString();
    }

    /**
     * Add a column to the natural join key column list.
     *
     * @param c the column to add
     */
    public void addNaturalJoinColumn(Column c) {
        if (naturalJoinColumns == null) {
            naturalJoinColumns = Utils.newSmallArrayList();
        }
        naturalJoinColumns.add(c);
    }

    /**
     * Check if the given column is a natural join column.
     *
     * @param c the column to check
     * @return true if this is a joined natural join column
     */
    public boolean isNaturalJoinColumn(Column c) {
        return naturalJoinColumns != null && naturalJoinColumns.indexOf(c) >= 0;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * Are there any index conditions that involve IN(...).
     *
     * @return whether there are IN(...) comparisons
     */
    public boolean hasInComparisons() {
        for (IndexCondition cond : indexConditions) {
            int compareType = cond.getCompareType();
            if (compareType == Comparison.IN_QUERY || compareType == Comparison.IN_LIST) {
                return true;
            }
        }
        return false;
    }

    /**
     * Lock the current row.
     */
    public boolean lockRow() {
        if (state == FOUND) {
            return table.tryLockRow(session, get());
        }
        return false;
    }

    public TableFilter getNestedJoin() {
        return nestedJoin;
    }

    /**
     * Set a nested joined table.
     *
     * @param filter the joined table filter
     */
    public void setNestedJoin(TableFilter filter) {
        nestedJoin = filter;
    }

    /**
     * Visit this and all joined or nested table filters.
     *
     * @param visitor the visitor
     */
    public void visit(TableFilterVisitor visitor) {
        TableFilter f = this;
        do {
            visitor.accept(f);
            TableFilter n = f.nestedJoin;
            if (n != null) {
                n.visit(visitor);
            }
            f = f.join;
        } while (f != null);
    }

    public ServerSession getSession() {
        return session;
    }

    /**
     * A visitor for table filters.
     */
    public interface TableFilterVisitor {

        /**
         * This method is called for each nested or joined table filter.
         *
         * @param f the filter
         */
        void accept(TableFilter f);
    }

    private boolean indexConditionsParsed = false;

    public void setIndexConditionsParsed(boolean parsed) {
        this.indexConditionsParsed = parsed;
    }

    public SearchRow getStartSearchRow() {
        if (!indexConditionsParsed) {
            indexConditionsParsed = true;
            cursor.parseIndexConditions(session, indexConditions);
        }
        return cursor.getStartSearchRow();
    }

    public SearchRow getEndSearchRow() {
        if (!indexConditionsParsed) {
            indexConditionsParsed = true;
            cursor.parseIndexConditions(session, indexConditions);
        }
        return cursor.getEndSearchRow();
    }

    public Map<List<String>, List<PageKey>> getNodeToPageKeyMap(ServerSession session) {
        if (!indexConditionsParsed) {
            indexConditionsParsed = true;
            cursor.parseIndexConditions(session, indexConditions);
        }
        return cursor.getNodeToPageKeyMap(session);
    }

    public void setPageKeys(List<PageKey> pageKeys) {
        cursor.setPageKeys(pageKeys);
    }

    public List<PageKey> getPageKeys() {
        return cursor.getPageKeys();
    }

    @Override
    public Value getExpressionValue(Session session, IExpression e, Object data) {
        setSession((ServerSession) session);
        set((org.lealone.db.result.Row) data);
        return e.getValue(session);
    }

    public int[] getColumnIndexes() {
        return columnIndexes;
    }

    public void setColumnIndexes(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public int[] createColumnIndexes(Expression... expressionArray) {
        int len = expressionArray.length;
        HashSet<Column> columnSet = new HashSet<>(len);
        for (int i = 0; i < len; i++) {
            expressionArray[i].getColumns(columnSet);
        }
        return createColumnIndexes(columnSet);
    }

    public int[] createColumnIndexes(Set<Column> columnSet) {
        int size = columnSet.size(); // size可能比原来的expressionArray大
        int[] columnIndexes = new int[size];
        int i = 0;
        for (Column c : columnSet) {
            if (c.getTable() == table) {
                columnIndexes[i++] = c.getColumnId(); // 索引从0开始，columnId也是从0开始
            }
        }
        if (i != size) {
            int[] tmp = new int[i];
            System.arraycopy(columnIndexes, 0, tmp, 0, i);
            columnIndexes = tmp;
        }

        Arrays.sort(columnIndexes);
        this.columnIndexes = columnIndexes;
        return columnIndexes;
    }
}
