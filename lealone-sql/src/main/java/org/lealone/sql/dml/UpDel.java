/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.StatementBase;
import org.lealone.sql.executor.YieldableLoopUpdateBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.evaluator.AlwaysTrueEvaluator;
import org.lealone.sql.expression.evaluator.ExpressionEvaluator;
import org.lealone.sql.expression.evaluator.ExpressionInterpreter;
import org.lealone.sql.optimizer.TableFilter;

// update和delete的基类
public abstract class UpDel extends ManipulationStatement {

    protected TableFilter tableFilter;
    protected Expression condition;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    protected Expression limitExpr;

    public UpDel(ServerSession session) {
        super(session);
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
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

    protected void appendPlanSQL(StatementBuilder buff) {
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(limitExpr.getSQL())).append(')');
        }
    }

    protected static abstract class YieldableUpDel extends YieldableLoopUpdateBase {

        protected final TableFilter tableFilter;
        protected final Table table;
        protected final int limitRows; // 如果是0，表示不删除任何记录；如果小于0，表示没有限制
        protected final ExpressionEvaluator conditionEvaluator;
        protected boolean hasNext;
        protected Row oldRow;

        public YieldableUpDel(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler,
                TableFilter tableFilter, Expression limitExpr, Expression condition) {
            super(statement, asyncHandler);
            this.tableFilter = tableFilter;
            table = tableFilter.getTable();
            limitRows = getLimitRows(limitExpr, session);
            if (condition == null)
                conditionEvaluator = new AlwaysTrueEvaluator();
            else
                conditionEvaluator = new ExpressionInterpreter(session, condition);
        }

        @Override
        protected boolean startInternal() {
            tableFilter.startQuery(session);
            tableFilter.reset();
            statement.setCurrentRowNumber(0);
            if (limitRows == 0)
                hasNext = false;
            else
                hasNext = next(); // 提前next，当发生行锁时可以直接用tableFilter的当前值重试
            return false;
        }

        protected boolean next() {
            return tableFilter.next();
        }

        protected void rebuildSearchRowIfNeeded() {
            if (oldRow != null) {
                // 如果oldRow已经删除了那么移到下一行
                if (tableFilter.rebuildSearchRow(session, oldRow) == null)
                    hasNext = next();
                oldRow = null;
            }
        }

        protected boolean tryLockRow(Row row, int[] lockColumns) {
            if (!table.tryLockRow(session, row, lockColumns)) {
                oldRow = row;
                return false;
            }
            return true;
        }

        private static int getLimitRows(Expression limitExpr, ServerSession session) {
            int limitRows = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            return limitRows;
        }
    }
}
