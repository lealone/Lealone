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
import org.lealone.sql.optimizer.TableIterator;

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

    @Override
    public int update() {
        return syncExecute(createYieldableUpdate(null));
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

        protected final Table table;
        private final int limitRows; // 如果是0，表示不删除任何记录；如果小于0，表示没有限制
        private final ExpressionEvaluator conditionEvaluator;
        private final TableIterator tableIterator;

        public YieldableUpDel(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler,
                TableFilter tableFilter, Expression limitExpr, Expression condition) {
            super(statement, asyncHandler);
            table = tableFilter.getTable();
            tableIterator = new TableIterator(session, tableFilter);

            int limitRows = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            this.limitRows = limitRows;

            if (condition == null)
                conditionEvaluator = new AlwaysTrueEvaluator();
            else
                conditionEvaluator = new ExpressionInterpreter(session, condition);
        }

        protected abstract int getRightMask();

        protected abstract int getTriggerType();

        protected abstract boolean upDelRow(Row oldRow);

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            session.getUser().checkRight(table, getRightMask());
            table.fire(session, getTriggerType(), true);
            statement.setCurrentRowNumber(0);
            tableIterator.start(limitRows);
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, getTriggerType(), false);
        }

        protected int[] getUpdateColumnIndexes() {
            return null;
        }

        @Override
        protected void executeLoopUpdate() {
            tableIterator.rebuildSearchRowIfNeeded();
            while (tableIterator.hasNext() && pendingException == null) {
                if (yieldIfNeeded(++loopCount)) {
                    return;
                }
                if (conditionEvaluator.getBooleanValue()) {
                    int ret = tableIterator.tryLockRow(getUpdateColumnIndexes());
                    if (ret < 0) {
                        continue;
                    } else if (ret == 0) { // 被其他事务锁住了
                        return;
                    }
                    Row row = tableIterator.getRow();
                    if (upDelRow(row)) {
                        if (limitRows > 0 && updateCount.get() >= limitRows) {
                            break;
                        }
                    }
                }
                tableIterator.next();
            }
            onLoopEnd();
        }
    }
}
