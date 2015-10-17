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
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * MERGE
 */
public class Merge extends StatementBase implements InsertOrMerge {

    protected Table table;
    protected Column[] columns;
    protected Column[] keys;
    protected final ArrayList<Expression[]> list = New.arrayList();
    protected Query query;
    protected StatementBase update;

    private List<Row> rows;

    public Merge(ServerSession session) {
        super(session);
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

    @Override
    public List<Row> getRows() {
        return rows;
    }

    @Override
    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public int update() {
        // 在集群模式下使用query时先不创建行，这会导致从其他表中把记录取过来
        if (query == null || isLocal())
            createRows();
        return org.lealone.sql.RouterHolder.getRouter().executeMerge(this);
    }

    @Override
    public int updateLocal() {
        return mergeRows();
    }

    @Override
    public Integer call() {
        return Integer.valueOf(mergeRows());
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
            int count = 0;
            Result rows = query.query(0);
            while (rows.next()) {
                Value[] values = rows.currentRow();
                try {
                    Row row = createRow(values);
                    if (row != null)
                        this.rows.add(row);
                } catch (DbException ex) {
                    throw setRow(ex, count + 1, getSQL(values));
                }
                ++count;
            }
        }
    }

    private int mergeRows() {
        if (rows == null)
            createRows();
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);

        int count = 0;
        Row newRow;
        setCurrentRowNumber(0);

        if (list.size() > 0) {
            for (int i = 0, size = rows.size(); i < size; i++) {
                newRow = rows.get(i);
                setCurrentRowNumber(++count);
                merge(newRow);
            }
        } else {
            count = 0;
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            table.lock(session, true, false);
            for (int i = 0, size = rows.size(); i < size; i++) {
                newRow = rows.get(i);
                setCurrentRowNumber(++count);
                merge(newRow);
            }
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
    public String getPlanSQL(List<Row> rows) {
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
        buff.resetCount();
        if (rows.size() > 0) {
            buff.append(" VALUES ");
            int i = 0;
            for (Row row : rows) {
                if (i++ > 0) {
                    buff.append(",");
                }
                buff.append('(');
                buff.resetCount();
                for (Column c : columns) {
                    buff.appendExceptFirst(", ");
                    Value v = row.getValue(c.getColumnId());
                    if (v == null) {
                        buff.append("DEFAULT");
                    } else {
                        if (v instanceof ValueString)
                            buff.append('\'').append(v.getString()).append('\'');
                        else
                            buff.append(v.getString());
                    }
                }
                buff.append(')');
            }
        }
        return buff.toString();
    }

    @Override
    public PreparedStatement prepare() {
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
    public boolean isTransactional() {
        return true;
    }

    @Override
    public Result queryMeta() {
        return null;
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
}
