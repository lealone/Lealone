/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.dml;

import com.codefollower.yourbase.api.Trigger;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.table.PlanItem;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.engine.UndoLogRecord;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.RowList;
import com.codefollower.yourbase.util.StringUtils;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueNull;

/**
 * This class represents the statement
 * DELETE
 */
public class Delete extends Prepared {

    private Expression condition;
    private TableFilter tableFilter;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    private Expression limitExpr;

    public Delete(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

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
                session.log(table, UndoLogRecord.DELETE, row);
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

    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
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
        return CommandInterface.DELETE;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    public boolean isCacheable() {
        return true;
    }

}
