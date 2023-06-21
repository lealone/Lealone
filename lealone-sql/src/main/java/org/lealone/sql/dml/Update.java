/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.executor.YieldableBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.ValueExpression;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends UpDel {

    private final ArrayList<Column> columns = Utils.newSmallArrayList();
    private final HashMap<Column, Expression> expressionMap = new HashMap<>();

    public Update(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.UPDATE;
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        if (expressionMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
        }
        columns.add(column);
        expressionMap.put(column, expression);
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(tableFilter.getPlanSQL(false)).append("\nSET\n    ");
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            buff.appendExceptFirst(",\n    ");
            buff.append(c.getName()).append(" = ").append(e.getSQL());
        }
        appendPlanSQL(buff);
        return buff.toString();
    }

    @Override
    public PreparedSQLStatement prepare() {
        int size = columns.size();
        HashSet<Column> columnSet = new HashSet<>(size + 1);
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
            condition.getColumns(columnSet);
        }
        for (int i = 0; i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            e.mapColumns(tableFilter, 0);
            expressionMap.put(c, e.optimize(session));

            columnSet.add(c);
            e.getColumns(columnSet); // 例如f1=f2*2;
        }
        tableFilter.createColumnIndexes(columnSet);
        tableFilter.preparePlan(session, 1);
        return this;
    }

    @Override
    public YieldableBase<Integer> createYieldableUpdate(
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableUpdate(this, asyncHandler);
    }

    private static class YieldableUpdate extends YieldableUpDel {

        final Update statement;
        final Column[] columns;
        final int[] updateColumnIndexes;
        final int columnCount;

        public YieldableUpdate(Update statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler, statement.tableFilter, statement.limitExpr,
                    statement.condition);
            this.statement = statement;
            columns = table.getColumns();
            columnCount = columns.length;

            int size = statement.columns.size();
            updateColumnIndexes = new int[size];
            for (int i = 0; i < size; i++) {
                updateColumnIndexes[i] = statement.columns.get(i).getColumnId();
            }
        }

        @Override
        protected int getRightMask() {
            return Right.UPDATE;
        }

        @Override
        protected int getTriggerType() {
            return Trigger.UPDATE;
        }

        @Override
        public int[] getUpdateColumnIndexes() {
            return updateColumnIndexes;
        }

        @Override
        protected boolean upDelRow(Row oldRow) {
            Row newRow = createNewRow(oldRow);
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = false;
            if (table.fireRow()) {
                done = table.fireBeforeRow(session, oldRow, newRow);
            }
            if (!done) {
                updateRow(oldRow, newRow);
            }
            return !done;
        }

        private Row createNewRow(Row oldRow) {
            Row newRow = table.getTemplateRow();
            newRow.setKey(oldRow.getKey()); // 复用原来的行号
            for (int i = 0; i < columnCount; i++) {
                Column column = columns[i];
                Expression newExpr = statement.expressionMap.get(column);
                Value newValue;
                if (newExpr == null) {
                    newValue = oldRow.getValue(i);
                } else if (newExpr == ValueExpression.getDefault()) {
                    newValue = table.getDefaultValue(session, column);
                } else {
                    newValue = column.convert(newExpr.getValue(session));
                }
                newRow.setValue(i, newValue);
            }
            return newRow;
        }

        private void updateRow(Row oldRow, Row newRow) {
            onPendingOperationStart();
            table.updateRow(session, oldRow, newRow, updateColumnIndexes, true).onComplete(ar -> {
                if (ar.isSucceeded() && table.fireRow()) {
                    table.fireAfterRow(session, oldRow, newRow, false);
                }
                onPendingOperationComplete(ar);
            });
        }
    }
}
