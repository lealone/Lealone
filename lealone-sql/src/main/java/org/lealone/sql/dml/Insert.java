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
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.result.Row;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * INSERT
 * 
 * @author H2 Group
 * @author zhh
 */
public class Insert extends ManipulationStatement implements ResultTarget {

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
        // 以同步的方式运行
        YieldableInsert yieldable = new YieldableInsert(this, null);
        yieldable.run();
        return yieldable.getResult();
    }

    @Deprecated
    public int updateOld() {
        session.getUser().checkRight(table, Right.INSERT);
        setCurrentRowNumber(0);
        table.fire(session, Trigger.INSERT, true);
        rowNumber = 0;
        int listSize = list.size();
        if (listSize > 0) {
            int columnLen = columns.length;
            for (int x = 0; x < listSize; x++) {
                Row newRow = table.getTemplateRow(); // newRow的长度是全表字段的个数，会>=columns的长度

                Expression[] expr = list.get(x);
                setCurrentRowNumber(x + 1);
                for (int i = 0; i < columnLen; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId(); // 从0开始
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        try {
                            Value v = c.convert(e.getValue(session));
                            newRow.setValue(index, v);
                        } catch (DbException ex) {
                            throw setRow(ex, x, getSQL(expr));
                        }
                    }
                }
                rowNumber++;
                table.validateConvertUpdateSequence(session, newRow);
                boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
                if (!done) {
                    // 直到事务commit或rollback时才解琐，见ServerSession.unlockAll()
                    table.lock(session, true, false);
                    table.addRow(session, newRow);
                    table.fireAfterRow(session, null, newRow, false);
                }
            }
        } else {
            table.lock(session, true, false);
            // 这种方式主要是避免循环两次，因为query内部己循环一次了
            if (insertFromSelect) {
                query.query(0, this); // 每遍历一行会回调下面的addRow方法
            } else {
                Result rows = query.query(0);
                while (rows.next()) {
                    addRow(rows.currentRow());
                }
                rows.close();
            }
        }
        table.fire(session, Trigger.INSERT, false);
        return rowNumber;
    }

    @Override
    public void addRow(Value[] values) {
        Row newRow = table.getTemplateRow();
        setCurrentRowNumber(++rowNumber);
        for (int j = 0, len = columns.length; j < len; j++) {
            Column c = columns[j];
            int index = c.getColumnId();
            try {
                Value v = c.convert(values[j]);
                newRow.setValue(index, v);
            } catch (DbException ex) {
                throw setRow(ex, rowNumber, getSQL(values));
            }
        }
        table.validateConvertUpdateSequence(session, newRow);
        boolean done = table.fireBeforeRow(session, null, newRow);
        if (!done) {
            table.addRow(session, newRow);
            table.fireAfterRow(session, null, newRow, false);
        }
    }

    @Override
    public int getRowCount() {
        return rowNumber;
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
    public double getCost() {
        return query != null ? query.getCost() : list.size();
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
    public YieldableInsert createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableInsert(this, asyncHandler);
    }

    private static class YieldableInsert extends YieldableListenableUpdateBase {

        final Insert statement;
        final Table table;
        final int listSize;

        int index;
        Result rows;

        public YieldableInsert(Insert statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            table = statement.table;
            listSize = statement.list.size();
        }

        @Override
        protected boolean startInternal() {
            session.getUser().checkRight(table, Right.INSERT);
            table.fire(session, Trigger.INSERT, true);
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                table.lock(session, true, false);
                rows = statement.query.query(0);
            }
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.INSERT, false);
        }

        @Override
        protected boolean executeAndListen() {
            if (rows == null) {
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
                    table.validateConvertUpdateSequence(session, newRow);
                    boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
                    if (!done) {
                        // 直到事务commit或rollback时才解琐，见ServerSession.unlockAll()
                        table.lock(session, true, false);
                        if (async)
                            table.tryAddRow(session, newRow, this);
                        else
                            table.addRow(session, newRow);
                        table.fireAfterRow(session, null, newRow, false);
                    }
                    if (async && yieldIfNeeded) {
                        return true;
                    }
                }
            } else {
                while (pendingOperationException == null && rows.next()) {
                    Value[] values = rows.currentRow();
                    Row newRow = table.getTemplateRow();
                    boolean yieldIfNeeded = statement.setCurrentRowNumber(++affectedRows);
                    for (int j = 0, len = statement.columns.length; j < len; j++) {
                        Column c = statement.columns[j];
                        int index = c.getColumnId();
                        try {
                            Value v = c.convert(values[j]);
                            newRow.setValue(index, v);
                        } catch (DbException ex) {
                            throw statement.setRow(ex, affectedRows, getSQL(values));
                        }
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    boolean done = table.fireBeforeRow(session, null, newRow);
                    if (!done) {
                        if (async)
                            table.tryAddRow(session, newRow, this);
                        else
                            table.addRow(session, newRow);
                        table.fireAfterRow(session, null, newRow, false);
                    }
                    if (async && yieldIfNeeded) {
                        return true;
                    }
                }
                rows.close();
            }
            loopEnd = true;
            return false;
        }
    }
}
