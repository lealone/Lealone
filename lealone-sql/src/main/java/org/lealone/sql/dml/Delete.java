/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.util.StringUtils;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.optimizer.PlanItem;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.yieldable.YieldableBase;
import org.lealone.sql.yieldable.YieldableListenableUpdateBase;
import org.lealone.transaction.Transaction;

/**
 * This class represents the statement
 * DELETE
 * 
 * @author H2 Group
 * @author zhh
 */
public class Delete extends ManipulationStatement {

    private TableFilter tableFilter;
    private Expression condition;

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

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    @Override
    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 0)
            return priority;

        priority = NORM_PRIORITY - 1;
        return priority;
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
            tableFilter.createColumnIndexes(condition);
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
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
    public YieldableBase<Integer> createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        if (!isLocal() && getSession().isShardingMode())
            return super.createYieldableUpdate(asyncHandler);
        else
            return new YieldableDelete(this, asyncHandler);
    }

    private static class YieldableDelete extends YieldableListenableUpdateBase {

        final Delete statement;
        final TableFilter tableFilter;
        final Table table;
        final int limitRows; // 如果是0，表示不删除任何记录；如果小于0，表示没有限制
        boolean hasNext;
        Row oldRow;

        public YieldableDelete(Delete statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            tableFilter = statement.tableFilter;
            table = tableFilter.getTable();
            limitRows = getLimitRows(statement.limitExpr, session);
        }

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            tableFilter.startQuery(session);
            tableFilter.reset();
            session.getUser().checkRight(table, Right.DELETE);
            table.fire(session, Trigger.DELETE, true);
            statement.setCurrentRowNumber(0);
            if (limitRows == 0)
                hasNext = false;
            else
                hasNext = tableFilter.next(); // 提前next，当发生行锁时可以直接用tableFilter的当前值重试
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.DELETE, false);
        }

        @Override
        protected boolean executeUpdate() {
            if (oldRow != null) {
                if (tableFilter.rebuildSearchRow(session, oldRow) == null)
                    hasNext = tableFilter.next();
                oldRow = null;
            }
            while (pendingOperationException == null && hasNext) {
                boolean yieldIfNeeded = statement.setCurrentRowNumber(affectedRows + 1);
                if (statement.condition == null || statement.condition.getBooleanValue(session)) {
                    Row row = tableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        if (async) {
                            int ret = table.tryRemoveRow(session, row, this);
                            if (ret == Transaction.OPERATION_NEED_RETRY) {
                                if (tableFilter.rebuildSearchRow(session, row) == null)
                                    hasNext = tableFilter.next();
                                continue;
                            } else if (ret != Transaction.OPERATION_COMPLETE) {
                                oldRow = row;
                                return true;
                            }
                        } else {
                            table.removeRow(session, row);
                        }
                        if (table.fireRow()) {
                            table.fireAfterRow(session, row, null, false);
                        }
                    }
                    affectedRows++;
                    if (limitRows > 0 && affectedRows >= limitRows) {
                        loopEnd = true;
                        return false;
                    }
                }
                hasNext = tableFilter.next();
                if (async && yieldIfNeeded) {
                    return true;
                }
            }
            loopEnd = true;
            return false;
        }
    }
}
