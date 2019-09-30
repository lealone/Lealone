/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.util.StringUtils;
import org.lealone.db.ServerSession;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Row;
import org.lealone.db.table.Table;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.optimizer.PlanItem;
import org.lealone.sql.optimizer.TableFilter;

/**
 * This class represents the statement
 * DELETE
 * 
 * @author H2 Group
 * @author zhh
 */
public class Delete extends ManipulationStatement {

    private Expression condition;
    private TableFilter tableFilter;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    private Expression limitExpr;

    public Delete(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DELETE;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    @Override
    public PreparedStatement prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
            tableFilter.createColumnIndexes(condition);
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
        cost = item.getCost();
        return this;
    }

    @Override
    public int update() {
        // 以同步的方式运行
        YieldableDelete yieldable = new YieldableDelete(this, null);
        yieldable.run();
        return yieldable.getResult();
    }

    @Override
    public String getPlanSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append("DELETE ");
        buff.append("FROM ").append(tableFilter.getPlanSQL(false));
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(limitExpr.getSQL())).append(')');
        }
        return buff.toString();
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 0)
            return priority;

        priority = NORM_PRIORITY - 1;
        return priority;
    }

    @Override
    public TableFilter getTableFilter() {
        return tableFilter;
    }

    @Override
    public YieldableDelete createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableDelete(this, asyncHandler);
    }

    private static class YieldableDelete extends YieldableUpdateBase {

        final Delete statement;
        final TableFilter tableFilter;
        final Table table;
        final int limitRows; // 如果是0，表示不删除任何记录；如果小于0，表示没有限制

        public YieldableDelete(Delete statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            tableFilter = statement.tableFilter;
            table = tableFilter.getTable();
            limitRows = getLimitRows(statement.limitExpr, session);
        }

        @Override
        protected boolean startInternal() {
            tableFilter.startQuery(session);
            tableFilter.reset();
            session.getUser().checkRight(table, Right.DELETE);
            table.fire(session, Trigger.DELETE, true);
            table.lock(session, true, false);
            statement.setCurrentRowNumber(0);
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.DELETE, false);
        }

        @Override
        protected boolean executeInternal() {
            if (delete()) {
                return true;
            }
            setResult(Integer.valueOf(affectedRows), affectedRows);
            return false;
        }

        private boolean delete() {
            if (limitRows == 0)
                return false;
            while (tableFilter.next()) {
                boolean yieldIfNeeded = statement.setCurrentRowNumber(affectedRows + 1);
                if (statement.condition == null || Boolean.TRUE.equals(statement.condition.getBooleanValue(session))) {
                    Row row = tableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        if (async)
                            yieldIfNeeded = table.tryRemoveRow(session, row) || yieldIfNeeded;
                        else
                            table.removeRow(session, row);
                        if (table.fireRow()) {
                            table.fireAfterRow(session, row, null, false);
                        }
                    }
                    affectedRows++;
                    if (limitRows > 0 && affectedRows >= limitRows) {
                        return false;
                    }
                }
                if (async && yieldIfNeeded) {
                    return true;
                }
            }
            return false;
        }
    }
}
