/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.StringUtils;
import org.lealone.db.ServerSession;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Row;
import org.lealone.db.result.RowList;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.optimizer.PlanItem;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.storage.PageKey;
import org.lealone.transaction.Transaction;

/**
 * This class represents the statement
 * DELETE
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
        tableFilter.startQuery(session);
        tableFilter.reset();
        Table table = tableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);
        RowList rows = new RowList(session);
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        try {
            setCurrentRowNumber(0);
            int count = 0;
            while (limitRows != 0 && tableFilter.next()) {
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    Row row = tableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        rows.add(row);
                    }
                    count++;
                    if (limitRows >= 0 && count >= limitRows) {
                        break;
                    }
                }
            }
            int rowScanCount = 0;
            for (rows.reset(); rows.hasNext();) {
                if ((++rowScanCount & 127) == 0) {
                    checkCanceled();
                }
                Row row = rows.next();
                table.removeRow(session, row);
            }
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    Row row = rows.next();
                    table.fireAfterRow(session, row, null, false);
                }
            }
            table.fire(session, Trigger.DELETE, false);
            return count;
        } finally {
            rows.close();
        }
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
    public YieldableUpdate createYieldableUpdate(List<PageKey> pageKeys, AsyncHandler<AsyncResult<Integer>> handler) {
        return new YieldableUpdate(this, pageKeys, handler);
    }

    @SuppressWarnings("unused")
    private static class YieldableUpdate2 extends YieldableBase<Integer> implements Transaction.Listener {

        private static enum State {
            find,
            update,
        }

        Delete statement;
        State state = State.find;
        Table table;
        RowList rows;
        int limitRows;
        int count;

        AtomicInteger counter = new AtomicInteger();

        public YieldableUpdate2(Delete statement, List<PageKey> pageKeys,
                AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, pageKeys, asyncHandler);
            this.statement = statement;
            callStop = false;
        }

        @Override
        protected boolean startInternal() {
            statement.tableFilter.startQuery(session);
            statement.tableFilter.reset();
            table = statement.tableFilter.getTable();
            session.getUser().checkRight(table, Right.DELETE);
            table.fire(session, Trigger.DELETE, true);
            table.lock(session, true, false);
            rows = new RowList(session);
            limitRows = -1;
            if (statement.limitExpr != null) {
                Value v = statement.limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            statement.setCurrentRowNumber(0);
            return false;
        }

        @Override
        protected void stopInternal() {
            try {
                if (table.fireRow()) {
                    for (rows.reset(); rows.hasNext();) {
                        Row row = rows.next();
                        table.fireAfterRow(session, row, null, false);
                    }
                }
                table.fire(session, Trigger.DELETE, false);
            } finally {
                rows.close();
            }
        }

        @Override
        protected boolean executeInternal() {
            switch (state) {
            case find:
                if (find()) {
                    return true;
                }
                counter.set(count);
                rows.reset();
                state = State.update;
            case update:
                if (update()) {
                    return true;
                }
            }
            return false;
        }

        private boolean find() {
            while (limitRows != 0 && statement.tableFilter.next()) {
                boolean yieldIfNeeded = statement.setCurrentRowNumber(rows.size() + 1);
                if (statement.condition == null || Boolean.TRUE.equals(statement.condition.getBooleanValue(session))) {
                    Row row = statement.tableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        rows.add(row);
                    }
                    count++;
                    if (limitRows >= 0 && count >= limitRows) {
                        break;
                    }
                }
                if (yieldIfNeeded) {
                    return true;
                }
            }
            return false;
        }

        private boolean update() {
            boolean yieldIfNeeded = false;
            int rowScanCount = 0;
            for (; rows.hasNext();) {
                if ((++rowScanCount & 127) == 0) {
                    statement.checkCanceled();
                    yieldIfNeeded = statement.yieldIfNeeded();
                }
                Row row = rows.next();
                table.removeRow(session, row);
                if (yieldIfNeeded) {
                    return true;
                }
            }
            setResult(Integer.valueOf(count), count);
            return false;
        }

        @Override
        public void partialUndo() {
            state = State.find;
        }

        @Override
        public void partialComplete() {
            if (counter.decrementAndGet() == 0) {
                stop();
            }
        }
    }

    private static class YieldableUpdate extends YieldableBase<Integer> implements Transaction.Listener {
        Delete statement;
        Table table;
        int limitRows;
        int count;

        AtomicInteger counter = new AtomicInteger();

        public YieldableUpdate(Delete statement, List<PageKey> pageKeys,
                AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, pageKeys, asyncHandler);
            this.statement = statement;
            callStop = false;
        }

        @Override
        protected boolean startInternal() {
            statement.tableFilter.startQuery(session);
            statement.tableFilter.reset();
            table = statement.tableFilter.getTable();
            session.getUser().checkRight(table, Right.DELETE);
            table.fire(session, Trigger.DELETE, true);
            table.lock(session, true, false);
            limitRows = -1;
            if (statement.limitExpr != null) {
                Value v = statement.limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            statement.setCurrentRowNumber(0);
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.DELETE, false);
        }

        @Override
        protected boolean executeInternal() {
            if (update()) {
                return true;
            }
            counter.set(count);
            return false;
        }

        private boolean update() {
            while (limitRows != 0 && statement.tableFilter.next()) {
                boolean yieldIfNeeded = statement.setCurrentRowNumber(count + 1);
                if (statement.condition == null || Boolean.TRUE.equals(statement.condition.getBooleanValue(session))) {
                    Row row = statement.tableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        table.removeRow(session, row, this);
                        if (table.fireRow()) {
                            table.fireAfterRow(session, row, null, false);
                        }
                    }
                    count++;
                    if (limitRows >= 0 && count >= limitRows) {
                        break;
                    }
                }
                if (yieldIfNeeded) {
                    return true;
                }
                setResult(Integer.valueOf(count), count);
            }
            return false;
        }

        @Override
        public void partialUndo() {
        }

        @Override
        public void partialComplete() {
            if (counter.decrementAndGet() == 0) {
                stop();
            }
        }
    }
}
