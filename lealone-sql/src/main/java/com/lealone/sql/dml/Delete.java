/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.dml;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.api.Trigger;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.auth.Right;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.executor.YieldableBase;

/**
 * This class represents the statement
 * DELETE
 */
public class Delete extends UpDel {

    public Delete(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DELETE;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder();
        buff.append("DELETE ");
        buff.append("FROM ").append(tableFilter.getPlanSQL(false));
        appendPlanSQL(buff);
        return buff.toString();
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
            tableFilter.createColumnIndexes(condition);
        }
        tableFilter.preparePlan(session, 1);
        return this;
    }

    @Override
    public YieldableBase<Integer> createYieldableUpdate(
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableDelete(this, asyncHandler);
    }

    private static class YieldableDelete extends YieldableUpDel {

        public YieldableDelete(Delete statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler, statement.tableFilter, statement.limitExpr,
                    statement.condition);
        }

        @Override
        protected int getRightMask() {
            return Right.DELETE;
        }

        @Override
        protected int getTriggerType() {
            return Trigger.DELETE;
        }

        @Override
        protected boolean upDelRow(Row oldRow) {
            boolean done = false;
            if (table.fireRow()) {
                done = table.fireBeforeRow(session, oldRow, null);
            }
            if (!done) {
                removeRow(oldRow);
            }
            return !done;
        }

        private void removeRow(Row row) {
            onPendingOperationStart();
            table.removeRow(session, row, true).onComplete(ar -> {
                if (ar.isSucceeded() && table.fireRow()) {
                    table.fireAfterRow(session, row, null, false);
                }
                onPendingOperationComplete(ar);
            });
        }
    }
}
