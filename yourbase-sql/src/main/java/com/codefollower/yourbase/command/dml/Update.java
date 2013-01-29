/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.dml;

import java.util.ArrayList;
import java.util.HashMap;

import com.codefollower.yourbase.api.Trigger;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.PlanItem;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.expression.Parameter;
import com.codefollower.yourbase.expression.ValueExpression;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.RowList;
import com.codefollower.yourbase.util.New;
import com.codefollower.yourbase.util.StatementBuilder;
import com.codefollower.yourbase.util.StringUtils;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueNull;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends Prepared {

    protected Expression condition;
    protected TableFilter tableFilter;

    /** The limit expression as specified in the LIMIT clause. */
    private Expression limitExpr;

    protected final ArrayList<Column> columns = New.arrayList();
    protected final HashMap<Column, Expression> expressionMap  = New.hashMap();

    public Update(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        if (expressionMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column
                    .getName());
        }
        columns.add(column);
        expressionMap.put(column, expression);
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    public int update() {
        tableFilter.startQuery(session);
        tableFilter.reset();
        RowList rows = new RowList(session);
        try {
            Table table = tableFilter.getTable();
            session.getUser().checkRight(table, Right.UPDATE);
            table.fire(session, Trigger.UPDATE, true);
            table.lock(session, true, false);
            int columnCount = table.getColumns().length;
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            Column[] columns = table.getColumns();
            int limitRows = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            while (tableFilter.next()) {
                setCurrentRowNumber(count+1);
                if (limitRows >= 0 && count >= limitRows) {
                    break;
                }
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    Row oldRow = tableFilter.get();
                    Row newRow = table.getTemplateRow();
                    for (int i = 0; i < columnCount; i++) {
                        Expression newExpr = expressionMap.get(columns[i]);
                        Value newValue;
                        if (newExpr == null) {
                            newValue = oldRow.getValue(i);
                        } else if (newExpr == ValueExpression.getDefault()) {
                            Column column = table.getColumn(i);
                            newValue = table.getDefaultValue(session, column);
                        } else {
                            Column column = table.getColumn(i);
                            newValue = column.convert(newExpr.getValue(session));
                        }
                        newRow.setValue(i, newValue);
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, oldRow, newRow);
                    }
                    if (!done) {
                        rows.add(oldRow);
                        rows.add(newRow);
                    }
                    count++;
                }
            }
            // TODO self referencing referential integrity constraints
            // don't work if update is multi-row and 'inversed' the condition!
            // probably need multi-row triggers with 'deleted' and 'inserted'
            // at the same time. anyway good for sql compatibility
            // TODO update in-place (but if the key changes,
            // we need to update all indexes) before row triggers

            // the cached row is already updated - we need the old values
            table.updateRows(this, session, rows);
            if (table.fireRow()) {
                rows.invalidateCache();
                for (rows.reset(); rows.hasNext();) {
                    Row o = rows.next();
                    Row n = rows.next();
                    table.fireAfterRow(session, o, n, false);
                }
            }
            table.fire(session, Trigger.UPDATE, false);
            return count;
        } finally {
            rows.close();
        }
    }

    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(tableFilter.getPlanSQL(false)).append("\nSET\n    ");
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            buff.appendExceptFirst(",\n    ");
            buff.append(c.getName()).append(" = ").append(e.getSQL());
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        return buff.toString();
    }

    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
        }
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            e.mapColumns(tableFilter, 0);
            expressionMap.put(c, e.optimize(session));
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    public boolean isTransactional() {
        return true;
    }

    public ResultInterface queryMeta() {
        return null;
    }

    public int getType() {
        return CommandInterface.UPDATE;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    public boolean isCacheable() {
        return true;
    }

}
