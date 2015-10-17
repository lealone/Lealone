/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.ServerSession;
import org.lealone.db.expression.Expression;
import org.lealone.db.expression.FunctionCall;
import org.lealone.db.expression.TableFunction;
import org.lealone.db.index.FunctionIndex;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.schema.Schema;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueResultSet;

/**
 * A table backed by a system or user-defined function that returns a result
 * set.
 */
public class FunctionTable extends Table {

    private final FunctionCall function;
    private final long rowCount;
    private Expression functionExpr;
    private LocalResult cachedResult;
    private Value cachedValue;

    public FunctionTable(Schema schema, ServerSession session, Expression functionExpr, FunctionCall function) {
        super(schema, 0, function.getName(), false, true);
        this.functionExpr = functionExpr;
        this.function = function;
        if (function instanceof TableFunction) {
            rowCount = ((TableFunction) function).getRowCount();
        } else {
            rowCount = Long.MAX_VALUE;
        }
        function.optimize(session);
        int type = function.getType();
        if (type != Value.RESULT_SET) {
            throw DbException.get(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        Expression[] args = function.getArgs();
        int numParams = args.length;
        Expression[] columnListArgs = new Expression[numParams];
        for (int i = 0; i < numParams; i++) {
            args[i] = args[i].optimize(session);
            columnListArgs[i] = args[i];
        }
        ValueResultSet template = function.getValueForColumnList(session, columnListArgs);
        if (template == null) {
            throw DbException.get(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ResultSet rs = template.getResultSet();
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            Column[] cols = new Column[columnCount];
            for (int i = 0; i < columnCount; i++) {
                cols[i] = new Column(meta.getColumnName(i + 1), DataType.getValueTypeFromResultSet(meta, i + 1),
                        meta.getPrecision(i + 1), meta.getScale(i + 1), meta.getColumnDisplaySize(i + 1));
            }
            setColumns(cols);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public boolean lock(ServerSession session, boolean exclusive, boolean forceLockEvenInMvcc) {
        // nothing to do
        return false;
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }

    @Override
    public void unlock(ServerSession s) {
        // nothing to do
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public Index addIndex(ServerSession session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void removeRow(ServerSession session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void truncate(ServerSession session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public boolean canDrop() {
        throw DbException.throwInternalError();
    }

    @Override
    public void addRow(ServerSession session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public String getTableType() {
        return null;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return new FunctionIndex(this, IndexColumn.wrap(columns));
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public boolean canGetRowCount() {
        return rowCount != Long.MAX_VALUE;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return rowCount;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    /**
     * Read the result from the function. This method buffers the result in a
     * temporary file.
     *
     * @param session the session
     * @return the result
     */
    public Result getResult(ServerSession session) {
        ValueResultSet v = getValueResultSet(session);
        if (v == null) {
            return null;
        }
        if (cachedResult != null && cachedValue == v) {
            cachedResult.reset();
            return cachedResult;
        }
        LocalResult result = LocalResult.read(session, v.getResultSet(), 0);
        if (function.isDeterministic()) {
            cachedResult = result;
            cachedValue = v;
        }
        return result;
    }

    /**
     * Read the result set from the function. This method doesn't cache.
     *
     * @param session the session
     * @return the result set
     */
    public ResultSet getResultSet(ServerSession session) {
        ValueResultSet v = getValueResultSet(session);
        return v == null ? null : v.getResultSet();
    }

    private ValueResultSet getValueResultSet(ServerSession session) {
        functionExpr = functionExpr.optimize(session);
        Value v = functionExpr.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return null;
        }
        return (ValueResultSet) v;
    }

    public boolean isBufferResultSetToLocalTemp() {
        return function.isBufferResultSetToLocalTemp();
    }

    @Override
    public long getMaxDataModificationId() {
        // TODO optimization: table-as-a-function currently doesn't know the
        // last modified date
        return Long.MAX_VALUE;
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public String getSQL() {
        return function.getSQL();
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    @Override
    public boolean canReference() {
        return false;
    }

}
