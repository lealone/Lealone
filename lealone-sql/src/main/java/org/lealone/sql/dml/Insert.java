/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.yieldable.YieldableBase;
import org.lealone.sql.yieldable.YieldableLoopUpdateBase;
import org.lealone.storage.replication.ReplicationConflictType;

/**
 * This class represents the statement
 * INSERT
 * 
 * @author H2 Group
 * @author zhh
 */
public class Insert extends ManipulationStatement {

    private Table table;
    private Column[] columns;
    private final ArrayList<Expression[]> list = new ArrayList<>();
    private Query query;
    private int rowNumber;
    private boolean insertFromSelect;

    public Insert(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.INSERT;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setInsertFromSelect(boolean value) {
        this.insertFromSelect = value;
    }

    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    @Override
    public void setLocal(boolean local) {
        super.setLocal(local);
        if (query != null)
            query.setLocal(local);
    }

    @Override
    public int getPriority() {
        if (rowNumber > 0)
            return priority;

        if (query != null || list.size() > 10)
            priority = NORM_PRIORITY - 1;
        else
            priority = MAX_PRIORITY;
        return priority;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(")\n");
        if (insertFromSelect) {
            buff.append("DIRECT ");
        }
        if (list.size() > 0) {
            buff.append("VALUES ");
            int row = 0;
            if (list.size() > 1) {
                buff.append('\n');
            }
            for (Expression[] expr : list) {
                if (row++ > 0) {
                    buff.append(",\n");
                }
                buff.append('(');
                buff.resetCount();
                for (Expression e : expr) {
                    buff.appendExceptFirst(", ");
                    if (e == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(e.getSQL());
                    }
                }
                buff.append(')');
            }
        } else {
            buff.append(query.getPlanSQL());
        }
        return buff.toString();
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (columns == null) {
            if (list.size() > 0 && list.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (list.size() > 0) {
            for (Expression[] expr : list) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0, len = expr.length; i < len; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        e = e.optimize(session);
                        if (e instanceof Parameter) {
                            Parameter p = (Parameter) e;
                            p.setColumn(columns[i]);
                        }
                        expr[i] = e;
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        return this;
    }

    @Override
    public int update() {
        YieldableInsert yieldable = new YieldableInsert(this, null);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableInsert createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableInsert(this, asyncHandler);
    }

    private static class YieldableInsert extends YieldableLoopUpdateBase implements ResultTarget {

        final Insert statement;
        final Table table;
        final int listSize;

        int index;
        Result rows;
        YieldableBase<Result> yieldableQuery;
        boolean isReplicationAppendMode;

        public YieldableInsert(Insert statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            table = statement.table;
            listSize = statement.list.size();
        }

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            session.getUser().checkRight(table, Right.INSERT);
            table.fire(session, Trigger.INSERT, true);
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                yieldableQuery = statement.query.createYieldableQuery(0, false, null,
                        statement.insertFromSelect ? this : null);
            }
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.INSERT, false);
        }

        @Override
        protected void executeLoopUpdate() {
            // 在复制模式下append记录时，先获得一个rowId区间，然后需要在客户端做rowId区间冲突检测，最后再返回正确的rowId区间
            if (!isReplicationAppendMode && session.isReplicationMode() && table.getScanIndex(session).isAppendMode()) {
                handleReplicationAppend();
                return;
            }
            if (yieldableQuery == null) {
                while (pendingException == null && index < listSize) {
                    addRowInternal(createNewRow());
                    if (yieldIfNeeded(index + 1)) {
                        return;
                    }
                    index++;
                }
                onLoopEnd();
            } else {
                if (statement.insertFromSelect) {
                    yieldableQuery.run();
                    if (yieldableQuery.isStopped()) {
                        onLoopEnd();
                    }
                } else {
                    if (rows == null) {
                        yieldableQuery.run();
                        if (!yieldableQuery.isStopped()) {
                            return;
                        }
                        rows = yieldableQuery.getResult();
                    }
                    while (pendingException == null && rows.next()) {
                        Value[] values = rows.currentRow();
                        if (addRow(values)) {
                            return;
                        }
                    }
                    rows.close();
                    onLoopEnd();
                }
            }
        }

        private void handleReplicationAppend() {
            long startKey = table.getScanIndex(session).getAndAddKey(listSize) + 1;
            session.setReplicationConflictType(ReplicationConflictType.APPEND);
            session.setStartKey(startKey);
            session.setEndKey(startKey + listSize);
            ServerSession s = table.getScanIndex(session).compareAndSetUncommittedSession(null, session);
            if (s != null) {
                session.setLockedExclusivelyBy(s, ReplicationConflictType.APPEND);
                session.setAppendIndex(table.getScanIndex(session));
            }
            session.setStatus(SessionStatus.WAITING);
            isReplicationAppendMode = true;
            if (asyncHandler != null) {
                asyncHandler.handle(new AsyncResult<>(-1));
            }
        }

        private Row createNewRow() {
            Row newRow = table.getTemplateRow(); // newRow的长度是全表字段的个数，会>=columns的长度
            Expression[] expr = statement.list.get(index);
            int columnLen = statement.columns.length;
            for (int i = 0; i < columnLen; i++) {
                Column c = statement.columns[i];
                int index = c.getColumnId(); // 从0开始
                Expression e = expr[i];
                if (e != null) {
                    // e can be null (DEFAULT)
                    e = e.optimize(session);
                    try {
                        Value v = c.convert(e.getValue(session));
                        newRow.setValue(index, v);
                    } catch (DbException ex) {
                        throw statement.setRow(ex, this.index + 1, getSQL(expr));
                    }
                }
            }
            if (isReplicationAppendMode) {
                newRow.setKey(session.getStartKey() + index);
            }
            return newRow;
        }

        private void addRowInternal(Row newRow) {
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
            if (!done) {
                pendingOperationCount.incrementAndGet();
                table.addRow(session, newRow).onComplete(ar -> {
                    if (ar.isSucceeded()) {
                        table.fireAfterRow(session, null, newRow, false);
                    }
                    onComplete(ar);
                });
            }
        }

        // 以下实现ResultTarget接口，可以在执行查询时，边查边增加新记录
        @Override
        public boolean addRow(Value[] values) {
            Row newRow = table.getTemplateRow();
            if (yieldIfNeeded(updateCount.get() + 1)) {
                return true;
            }
            for (int j = 0, len = statement.columns.length; j < len; j++) {
                Column c = statement.columns[j];
                int index = c.getColumnId();
                try {
                    Value v = c.convert(values[j]);
                    newRow.setValue(index, v);
                } catch (DbException ex) {
                    throw statement.setRow(ex, updateCount.get() + 1, getSQL(values));
                }
            }
            addRowInternal(newRow);
            return false;
        }

        @Override
        public int getRowCount() {
            return updateCount.get();
        }
    }
}
