/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.dml;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.lealone.api.ErrorCode;
import org.lealone.api.Trigger;
import org.lealone.command.Command;
import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Session;
import org.lealone.expression.Expression;
import org.lealone.expression.Parameter;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;
import org.lealone.result.ResultTarget;
import org.lealone.result.Row;
import org.lealone.util.New;
import org.lealone.util.StatementBuilder;
import org.lealone.value.Value;

/**
 * This class represents the statement
 * INSERT
 */
public class Insert extends Prepared implements ResultTarget, Callable<Integer> {

    protected Table table;
    protected Column[] columns;
    //TODO
    //protected Expression[] first; //大多数情况下每次都只有一条记录
    protected ArrayList<Expression[]> list = New.arrayList();
    protected Query query;
    protected boolean sortedInsertMode;
    protected int rowNumber;
    protected boolean insertFromSelect;

    //protected InsertOrMergeRouter router;

    public Insert(Session session) {
        super(session);
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public List<Row> getRows() {
        return rows;
    }

    /**
     * Add a row to this merge statement.
     *
     * @param expr the list of values
     */
    public void addRow(Expression[] expr) {
        //        if (first == null)
        //            first = expr;
        //        else {
        //            if (list == null)
        //                list = New.arrayList();
        //            list.add(expr);
        //        }

        list.add(expr);
    }

    //    private ArrayList<Value> partitionKeys;
    //    private final int partitionKeyIndex = -1;

    //    private void parsePartitionKeys() {
    //        partitionKeys = New.arrayList();
    //        if (partitionKeyIndex == -1 && query != null) {
    //            int index = -1;
    //            for (Column c : columns) {
    //                index++;
    //                if (c.isRowKeyColumn()) {
    //                    partitionKeyIndex = index;
    //                    break;
    //                }
    //            }
    //        }
    //
    //        int listSize = list.size();
    //        if (listSize > 0) {
    //            if (partitionKeyIndex == -1) {
    //                for (int i = 0; i < listSize; i++) {
    //                    partitionKeys.add(ValueUuid.getNewRandom());
    //                }
    //            } else {
    //                Expression[] expr;
    //                Expression e;
    //                Value v;
    //                Column c = columns[partitionKeyIndex];
    //                for (int i = 0; i < listSize; i++) {
    //                    expr = list.get(i);
    //                    v = null;
    //                    if (expr != null && expr.length > 0) {
    //                        e = expr[partitionKeyIndex];
    //                        if (e != null) {
    //                            // e can be null (DEFAULT)
    //                            e = e.optimize(session);
    //                            v = c.convert(e.getValue(session));
    //                        }
    //                    }
    //
    //                    if (v == null)
    //                        partitionKeys.add(ValueUuid.getNewRandom());
    //                    else
    //                        partitionKeys.add(v);
    //                }
    //            }
    //        }
    //    }

    @Override
    public int update() {
        createRows();

        if (isLocal()) {
            Index index = null;
            if (sortedInsertMode) {
                index = table.getScanIndex(session);
                index.setSortedInsertMode(true);
            }

            try {
                return insertRows();
            } finally {
                if (index != null) {
                    index.setSortedInsertMode(false);
                }
            }
        } else {
            //parsePartitionKeys();
            return Session.getRouter().executeInsert(this);
        }
    }

    private List<Row> rows;

    private void createRows() {
        int listSize = list.size();
        if (listSize > 0) {
            rows = New.arrayList(listSize);
            for (int x = 0; x < listSize; x++) {
                Expression[] expr = list.get(x);
                try {
                    createRow(expr, x);
                } catch (DbException ex) {
                    throw setRow(ex, x + 1, getSQL(expr));
                }
            }
        } else {
            rows = New.arrayList();
            rowNumber = 0;
            if (insertFromSelect) {
                query.query(0, this);
            } else {
                ResultInterface rows = query.query(0);
                while (rows.next()) {
                    Value[] r = rows.currentRow();
                    addRow(r);
                }
                rows.close();
            }
        }
    }

    private int insertRows() {
        session.getUser().checkRight(table, Right.INSERT);
        setCurrentRowNumber(0);
        table.fire(session, Trigger.INSERT, true);
        rowNumber = 0;

        Row newRow;
        for (int i = 0, size = rows.size(); i < size; i++) {
            newRow = rows.get(i);
            setCurrentRowNumber(++rowNumber);
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = table.fireBeforeRow(session, null, newRow);
            if (!done) {
                table.lock(session, true, false);
                table.addRow(session, newRow);
                table.fireAfterRow(session, null, newRow, false);
            }
        }

        table.fire(session, Trigger.INSERT, false);
        return rowNumber;
    }

    //    private int insertRows2() {
    //        session.getUser().checkRight(table, Right.INSERT);
    //        setCurrentRowNumber(0);
    //        table.fire(session, Trigger.INSERT, true);
    //        rowNumber = 0;
    //        int listSize = list.size();
    //        if (listSize > 0) {
    //            for (int x = 0; x < listSize; x++) {
    //                Expression[] expr = list.get(x);
    //                Row newRow;
    //                try {
    //                    newRow = createRow(expr, x);
    //                    if (newRow == null) {
    //                        continue;
    //                    }
    //                } catch (DbException ex) {
    //                    throw setRow(ex, rowNumber + 1, getSQL(expr));
    //                }
    //                setCurrentRowNumber(++rowNumber);
    //                table.validateConvertUpdateSequence(session, newRow);
    //                boolean done = table.fireBeforeRow(session, null, newRow);
    //                if (!done) {
    //                    table.lock(session, true, false);
    //                    table.addRow(session, newRow);
    //                    table.fireAfterRow(session, null, newRow, false);
    //                }
    //            }
    //        } else {
    //            table.lock(session, true, false);
    //            if (insertFromSelect) {
    //                query.query(0, this);
    //            } else {
    //                ResultInterface rows = query.query(0);
    //                while (rows.next()) {
    //                    Value[] r = rows.currentRow();
    //                    addRow(r);
    //                }
    //                rows.close();
    //            }
    //        }
    //        table.fire(session, Trigger.INSERT, false);
    //        return rowNumber;
    //    }

    @Override
    public void addRow(Value[] values) {
        ++rowNumber;
        try {
            createRow(values);
        } catch (DbException ex) {
            throw setRow(ex, rowNumber, getSQL(values));
        }
        //        Row newRow;
        //        try {
        //            newRow = createRow(values);
        //            if (newRow == null) {
        //                return;
        //            }
        //        } catch (DbException ex) {
        //            throw setRow(ex, rowNumber + 1, getSQL(values));
        //        }
        //        setCurrentRowNumber(++rowNumber);
        //        table.validateConvertUpdateSequence(session, newRow);
        //        boolean done = table.fireBeforeRow(session, null, newRow);
        //        if (!done) {
        //            table.addRow(session, newRow);
        //            table.fireAfterRow(session, null, newRow, false);
        //        }
    }

    //子类有可能要用rowId
    protected Row createRow(Expression[] expr, int rowId) {
        Row row = table.getTemplateRow();
        //row.setTransactionId(session.getTransaction().getTransactionId());
        for (int i = 0, len = columns.length; i < len; i++) {
            Column c = columns[i];
            int index = c.getColumnId();
            Expression e = expr[i];
            if (e != null) {
                // e can be null (DEFAULT)
                e = e.optimize(session);
                Value v = c.convert(e.getValue(session));
                row.setValue(index, v, c);
            }
        }
        rows.add(row);
        return row;
    }

    protected Row createRow(Value[] values) {
        Row row = table.getTemplateRow();
        for (int j = 0, len = columns.length; j < len; j++) {
            Column c = columns[j];
            int index = c.getColumnId();
            Value v = c.convert(values[j]);
            row.setValue(index, v, c);
        }
        rows.add(row);
        return row;
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
        if (sortedInsertMode) {
            buff.append("SORTED ");
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

    public String getPlanSQL(List<Row> rows) {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") ");

        if (sortedInsertMode) {
            buff.append("SORTED ");
        }

        buff.resetCount();
        if (rows.size() > 0) {
            buff.append("VALUES ");
            int i = 0;
            for (Row row : rows) {
                buff.appendExceptFirst(", ");
                if (i++ > 0) {
                    buff.append(",");
                }
                buff.append('(');
                buff.resetCount();
                for (Value v : row.getValueList()) {
                    buff.appendExceptFirst(", ");
                    if (v == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(v.getString());
                    }
                }
                buff.append(')');
            }
        }
        return buff.toString();
    }

    @Override
    public void prepare() {
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
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    @Override
    public int getType() {
        return CommandInterface.INSERT;
    }

    public void setInsertFromSelect(boolean value) {
        this.insertFromSelect = value;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public Integer call() {
        return Integer.valueOf(insertRows());
    }
}
