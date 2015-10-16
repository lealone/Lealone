/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;
import java.util.List;

import org.lealone.api.ErrorCode;
import org.lealone.api.Trigger;
import org.lealone.common.message.DbException;
import org.lealone.common.util.New;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.CommandInterface;
import org.lealone.db.Session;
import org.lealone.db.auth.Right;
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.result.Row;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.sql.Command;
import org.lealone.sql.Prepared;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * INSERT
 */
public class Insert extends Prepared implements ResultTarget, InsertOrMerge {

    protected Table table;
    protected Column[] columns;
    // TODO
    // protected Expression[] first; //大多数情况下每次都只有一条记录
    protected ArrayList<Expression[]> list = New.arrayList();
    protected Query query;
    protected boolean sortedInsertMode;
    protected int rowNumber;
    protected boolean insertFromSelect;

    private List<Row> rows;

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

    @Override
    public Table getTable() {
        return table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public List<Row> getRows() {
        return rows;
    }

    @Override
    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    /**
     * Add a row to this merge statement.
     *
     * @param expr the list of values
     */
    public void addRow(Expression[] expr) {
        // if (first == null)
        // first = expr;
        // else {
        // if (list == null)
        // list = New.arrayList();
        // list.add(expr);
        // }

        list.add(expr);
    }

    @Override
    public int update() {
        // 在集群模式下使用query时先不创建行，这会导致从其他表中把记录取过来
        if (query == null || isLocal())
            createRows();
        return org.lealone.sql.RouterHolder.getRouter().executeInsert(this);
    }

    @Override
    public int updateLocal() {
        if (rows == null)
            createRows();
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
    }

    @Override
    public Integer call() {
        return Integer.valueOf(updateLocal());
    }

    protected void createRows() {
        int listSize = list.size();
        if (listSize > 0) {
            rows = New.arrayList(listSize);
            for (int x = 0; x < listSize; x++) {
                Expression[] expr = list.get(x);
                try {
                    Row row = createRow(expr, x);
                    if (row != null)
                        rows.add(row);
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
                Result rows = query.query(0);
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

    @Override
    public void addRow(Value[] values) {
        ++rowNumber;
        try {
            Row row = createRow(values);
            if (row != null)
                rows.add(row);
        } catch (DbException ex) {
            throw setRow(ex, rowNumber, getSQL(values));
        }
    }

    @Override
    public int getRowCount() {
        return rowNumber;
    }

    // 子类有可能要用rowId
    protected Row createRow(Expression[] expr, int rowId) {
        Row row = table.getTemplateRow();
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
        return row;
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

    @Override
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
                if (i++ > 0) {
                    buff.append(",");
                }
                buff.append('(');
                buff.resetCount();
                for (Column c : columns) {
                    Value v = row.getValue(c.getColumnId());
                    buff.appendExceptFirst(", ");
                    if (v == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(v.getSQL());
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
    public Result queryMeta() {
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

    @Override
    public boolean isBatch() {
        // 因为GlobalUniqueIndex是通过独立的唯一索引表实现的，如果包含GlobalUniqueIndex，
        // 那么每次往主表中增加一条记录时，都会同时往唯一索引表中加一条记录，所以也是批量的
        return (query != null && query.isBatchForInsert()) || list.size() > 1 || table.containsGlobalUniqueIndex();
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public void setLocal(boolean local) {
        super.setLocal(local);
        if (query != null)
            query.setLocal(local);
    }

    @Override
    public List<Long> getRowVersions() {
        if (rows == null)
            return null;
        ArrayList<Long> list = new ArrayList<>(rows.size());
        for (Row row : rows)
            list.add(table.getRowVersion(row.getKey()));
        return list;
    }
}
