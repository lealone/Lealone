/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.dml;

import java.util.ArrayList;

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
import org.lealone.engine.UndoLogRecord;
import org.lealone.expression.Expression;
import org.lealone.expression.Parameter;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;
import org.lealone.result.Row;
import org.lealone.util.New;
import org.lealone.util.StatementBuilder;
import org.lealone.value.Value;

/**
 * This class represents the statement
 * MERGE
 */
public class Merge extends Prepared {

    protected Table table;
    protected Column[] columns;
    protected Column[] keys;
    protected final ArrayList<Expression[]> list = New.arrayList();
    protected Query query;
    protected Prepared update;

    public Merge(Session session) {
        super(session);
    }

    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
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

    /**
     * Add a row to this merge statement.
     *
     * @param expr the list of values
     */
    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    public int update() {
        int count;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        setCurrentRowNumber(0);
        if (list.size() > 0) {
            count = 0;
            for (int x = 0, size = list.size(); x < size; x++) {
                Expression[] expr = list.get(x);
                Row newRow;
                try {
                    newRow = createRow(expr, x);
                    if (newRow == null) {
                        continue;
                    }
                } catch (DbException ex) {
                    throw setRow(ex, count + 1, getSQL(expr));
                }
                setCurrentRowNumber(++count);
                merge(newRow);
            }
        } else {
            ResultInterface rows = query.query(0);
            count = 0;
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            table.lock(session, true, false);
            while (rows.next()) {
                Value[] values = rows.currentRow();
                Row newRow;
                try {
                    newRow = createRow(values);
                    if (newRow == null) {
                        continue;
                    }
                } catch (DbException ex) {
                    throw setRow(ex, count + 1, getSQL(values));
                }
                setCurrentRowNumber(++count);
                merge(newRow);
            }
            rows.close();
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }
        return count;
    }

    protected Row createRow(Expression[] expr, int rowId) {
        Row newRow = table.getTemplateRow();
        for (int i = 0, len = columns.length; i < len; i++) {
            Column c = columns[i];
            int index = c.getColumnId();
            Expression e = expr[i];
            if (e != null) {
                // e can be null (DEFAULT)
                try {
                    Value v = c.convert(e.getValue(session));
                    newRow.setValue(index, v);
                } catch (DbException ex) {
                    throw setRow(ex, rowId, getSQL(expr));
                }
            }
        }
        return newRow;
    }

    protected Row createRow(Value[] values) {
        Row newRow = table.getTemplateRow();
        for (int j = 0; j < columns.length; j++) {
            Column c = columns[j];
            int index = c.getColumnId();
            Value v = c.convert(values[j]);
            newRow.setValue(index, v);
        }
        return newRow;
    }

    private void merge(Row row) {
        ArrayList<Parameter> k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            Parameter p = k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
            }
            Parameter p = k.get(columns.length + i);
            p.setValue(v);
        }
        int count = update.update();
        if (count == 0) {
            try {
                table.validateConvertUpdateSequence(session, row);
                boolean done = table.fireBeforeRow(session, null, row);
                if (!done) {
                    table.lock(session, true, false);
                    table.addRow(session, row);
                    session.log(table, UndoLogRecord.INSERT, row);
                    table.fireAfterRow(session, null, row, false);
                }
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // possibly a concurrent merge or insert
                    Index index = (Index) e.getSource();
                    if (index != null) {
                        // verify the index columns match the key
                        Column[] indexColumns = index.getColumns();
                        boolean indexMatchesKeys = false;
                        if (indexColumns.length <= keys.length) {
                            for (int i = 0; i < indexColumns.length; i++) {
                                if (indexColumns[i] != keys[i]) {
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
    }

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
            buff.append("VALUES ");
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
        update = session.prepare(sql);
    }

    public boolean isTransactional() {
        return true;
    }

    public ResultInterface queryMeta() {
        return null;
    }

    public int getType() {
        return CommandInterface.MERGE;
    }

    public boolean isCacheable() {
        return true;
    }

}
