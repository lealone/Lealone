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
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.yieldable.YieldableBase;
import org.lealone.sql.yieldable.YieldableLoopUpdateBase;

/**
 * This class represents the statement
 * MERGE
 * 
 * @author H2 Group
 * @author zhh
 */
public class Merge extends ManipulationStatement {

    private Table table;
    private Column[] columns;
    private Column[] keys;
    private final ArrayList<Expression[]> list = new ArrayList<>();
    private Query query;
    private StatementBase update;

    public Merge(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.MERGE;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public void setLocal(boolean local) {
        super.setLocal(local);
        if (query != null)
            query.setLocal(local);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setKeys(Column[] keys) {
        this.keys = keys;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 0)
            return priority;

        priority = NORM_PRIORITY - 1;
        return priority;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (keys != null) {
            buff.append(" KEY(");
            buff.resetCount();
            for (Column c : keys) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        }
        buff.append('\n');
        if (list.size() > 0) {
            buff.append(" VALUES ");
            int row = 0;
            for (Expression[] expr : list) {
                if (row++ > 0) {
                    buff.append(", ");
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
                for (int i = 0; i < expr.length; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        expr[i] = e.optimize(session);
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (keys == null) {
            Index idx = table.getPrimaryKey();
            if (idx == null) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(table.getSQL()).append(" SET ");
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        buff.resetCount();
        for (Column c : keys) {
            buff.appendExceptFirst(" AND ");
            buff.append(c.getSQL()).append("=?");
        }
        String sql = buff.toString();
        update = (StatementBase) session.prepareStatement(sql);

        return this;
    }

    @Override
    public int update() {
        YieldableMerge yieldable = new YieldableMerge(this, null);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableMerge createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableMerge(this, asyncHandler);
    }

    private static class YieldableMerge extends YieldableLoopUpdateBase {

        final Merge statement;
        final Table table;
        final int listSize;

        int index;
        Result rows;
        YieldableBase<Result> yieldableQuery;

        public YieldableMerge(Merge statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            table = statement.table;
            listSize = statement.list.size();
        }

        @Override
        protected boolean startInternal() {
            session.getUser().checkRight(table, Right.INSERT);
            session.getUser().checkRight(table, Right.UPDATE);
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                if (!table.trySharedLock(session))
                    return true;
                yieldableQuery = statement.query.createYieldableQuery(0, false, null, null);
                table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            }
            return false;
        }

        @Override
        protected void stopInternal() {
            // do nothing
        }

        @Override
        protected void executeLoopUpdate() {
            if (yieldableQuery == null) {
                int columnLen = statement.columns.length;
                for (; pendingOperationException == null && index < listSize; index++) {
                    Row newRow = table.getTemplateRow(); // newRow的长度是全表字段的个数，会>=columns的长度
                    Expression[] expr = statement.list.get(index);
                    boolean yieldIfNeeded = statement.setCurrentRowNumber(index + 1);
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
                                throw statement.setRow(ex, index, getSQL(expr));
                            }
                        }
                    }
                    affectedRows++;
                    if (merge(newRow, yieldIfNeeded)) {
                        return;
                    }
                }
            } else {
                if (rows == null) {
                    yieldableQuery.run();
                    rows = yieldableQuery.getResult();
                }
                while (pendingOperationException == null && rows.next()) {
                    affectedRows++;
                    Value[] r = rows.currentRow();
                    Row newRow = table.getTemplateRow();
                    boolean yieldIfNeeded = statement.setCurrentRowNumber(affectedRows);
                    for (int j = 0; j < statement.columns.length; j++) {
                        Column c = statement.columns[j];
                        int index = c.getColumnId();
                        try {
                            Value v = c.convert(r[j]);
                            newRow.setValue(index, v);
                        } catch (DbException ex) {
                            throw statement.setRow(ex, affectedRows, getSQL(r));
                        }
                    }
                    if (merge(newRow, yieldIfNeeded)) {
                        return;
                    }
                }
                rows.close();
                table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
            }
            loopEnd = true;
        }

        private boolean merge(Row row, boolean yieldIfNeeded) {
            ArrayList<Parameter> k = statement.update.getParameters();
            for (int i = 0; i < statement.columns.length; i++) {
                Column col = statement.columns[i];
                Value v = row.getValue(col.getColumnId());
                if (v == null)
                    v = ValueNull.INSTANCE;
                Parameter p = k.get(i);
                p.setValue(v);
            }
            for (int i = 0; i < statement.keys.length; i++) {
                Column col = statement.keys[i];
                Value v = row.getValue(col.getColumnId());
                if (v == null) {
                    throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
                }
                Parameter p = k.get(statement.columns.length + i);
                p.setValue(v);
            }
            // 先更新，如果没有记录被更新，说明是一条新的记录，接着再插入
            int count = statement.update.update();
            if (count == 0) {
                try {
                    if (addRowInternal(row, yieldIfNeeded)) {
                        return true;
                    }
                } catch (DbException e) {
                    if (e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                        // possibly a concurrent merge or insert
                        Index index = (Index) e.getSource();
                        if (index != null) {
                            // verify the index columns match the key
                            Column[] indexColumns = index.getColumns();
                            boolean indexMatchesKeys = false;
                            if (indexColumns.length <= statement.keys.length) {
                                for (int i = 0; i < indexColumns.length; i++) {
                                    if (indexColumns[i] != statement.keys[i]) {
                                        indexMatchesKeys = false;
                                        break;
                                    }
                                }
                            }
                            if (indexMatchesKeys) {
                                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
                            }
                        }
                    }
                    throw e;
                }
            } else if (count != 1) {
                throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getSQL());
            }
            return false;
        }

        private boolean addRowInternal(Row newRow, boolean yieldIfNeeded) {
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
            if (!done) {
                // 直到事务commit或rollback时才解琐，见ServerSession.unlockAll()
                if (!table.trySharedLock(session)) {
                    session.setStatus(SessionStatus.WAITING);
                    return true;
                }
                pendingOperationCount.incrementAndGet();

                table.addRow(session, newRow).onComplete(ar -> {
                    pendingOperationCount.decrementAndGet();
                    if (ar.isSucceeded()) {
                        table.fireAfterRow(session, null, newRow, false);
                        updateCount.incrementAndGet();
                    } else {
                        pendingOperationException = ar.getCause();
                    }

                    if (isCompleted()) {
                        setResult(updateCount.get());
                    } else if (statementExecutor != null) {
                        statementExecutor.wakeUp();
                    }
                });
            }
            return false;
        }
    }
}
