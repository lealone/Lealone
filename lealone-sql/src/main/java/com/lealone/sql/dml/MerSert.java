/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.dml;

import java.util.ArrayList;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.DataHandler;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.command.CommandParameter;
import com.lealone.db.result.Result;
import com.lealone.db.result.ResultTarget;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.executor.YieldableLoopUpdateBase;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.Query;

// merge和insert的基类
public abstract class MerSert extends ManipulationStatement {

    protected Table table;
    protected Column[] columns;
    protected Query query;
    protected final ArrayList<Expression[]> list = new ArrayList<>();

    protected List<Value[]> batchParameterValues;

    public MerSert(ServerSession session) {
        super(session);
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

    public Query getQuery() {
        return query;
    }

    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    public void clearRows() {
        list.clear();
    }

    public void setBatchParameterValues(List<Value[]> batchParameterValues) {
        this.batchParameterValues = batchParameterValues;
    }

    @Override
    public void checkParameters() {
        if (batchParameterValues == null) {
            super.checkParameters();
        }
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 0)
            return priority;

        if (query != null || list.size() > 10)
            priority = NORM_PRIORITY - 1;
        else
            priority = MAX_PRIORITY;
        return priority;
    }

    protected void getValuesPlanSQL(StatementBuilder buff) {
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

    protected static abstract class YieldableMerSert extends YieldableLoopUpdateBase
            implements ResultTarget {

        final MerSert statement;
        final Table table;
        final int listSize;

        int index;
        YieldableBase<Result> yieldableQuery;

        public YieldableMerSert(MerSert statement, AsyncResultHandler<Integer> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            table = statement.table;
            listSize = statement.batchParameterValues != null ? statement.batchParameterValues.size()
                    : statement.list.size();
        }

        @Override
        protected boolean startInternal() {
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                yieldableQuery = statement.query.createYieldableQuery(0, false, null, this);
            }
            return false;
        }

        @Override
        protected void executeLoopUpdate() {
            if (table.containsLargeObject()) {
                DataHandler dh = session.getDataHandler();
                session.setDataHandler(table.getDataHandler()); // lob字段通过FILE_READ函数赋值时会用到
                try {
                    executeLoopUpdate0();
                } finally {
                    session.setDataHandler(dh);
                }
            } else {
                executeLoopUpdate0();
            }
        }

        private void executeLoopUpdate0() {
            if (yieldableQuery == null) {
                while (pendingException == null && index < listSize) {
                    merSert(createNewRow());
                    if (yieldIfNeeded(++index)) {
                        return;
                    }
                }
                onLoopEnd();
            } else {
                yieldableQuery.run();
                if (yieldableQuery.isStopped()) {
                    onLoopEnd();
                }
            }
        }

        protected Row createNewRow() {
            Row newRow = table.getTemplateRow(); // newRow的长度是全表字段的个数，会>=columns的长度
            Expression[] expr;
            if (statement.batchParameterValues != null) {
                expr = statement.list.get(0);
                Value[] parameters = statement.batchParameterValues.get(index);
                List<? extends CommandParameter> params = statement.getParameters();
                for (int i = 0, size = parameters.length; i < size; i++) {
                    CommandParameter p = params.get(i);
                    p.setValue(parameters[i]);
                }
            } else {
                expr = statement.list.get(index);
            }
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
            return newRow;
        }

        protected Row createNewRow(Value[] values) {
            Row newRow = table.getTemplateRow();
            for (int i = 0, len = statement.columns.length; i < len; i++) {
                Column c = statement.columns[i];
                int index = c.getColumnId();
                try {
                    Value v = c.convert(values[i]);
                    newRow.setValue(index, v);
                } catch (DbException ex) {
                    throw statement.setRow(ex, updateCount.get() + 1, getSQL(values));
                }
            }
            return newRow;
        }

        protected void addRowInternal(Row newRow) {
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
            if (!done) {
                onPendingOperationStart();
                table.addRow(session, newRow, ar -> {
                    if (ar.isSucceeded()) {
                        try {
                            // 有可能抛出异常
                            table.fireAfterRow(session, null, newRow, false);
                        } catch (Throwable e) {
                            setPendingException(e);
                        }
                    }
                    onPendingOperationComplete(ar);
                });
            }
        }

        protected abstract void merSert(Row row);

        // 以下实现ResultTarget接口，可以在执行查询时，边查边增加新记录
        @Override
        public boolean addRow(Value[] values) {
            merSert(createNewRow(values));
            if (yieldIfNeeded(updateCount.get() + 1)) {
                return true;
            }
            return false;
        }

        @Override
        public int getRowCount() {
            return updateCount.get();
        }

        @Override
        public boolean optimizeInsertFromSelect() {
            // 对于insert into t select * from t这样的场景需要禁用优化
            // 因为会产生无限循环
            if (statement.query != null) {
                for (TableFilter tf : statement.query.getFilters()) {
                    if (table.getId() == tf.getTable().getId())
                        return false;
                }
                return true;
            }
            return false;
        }
    }
}
