/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.lealone.client.command.ClientPreparedSQLCommand;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.db.value.BlobBase;
import org.lealone.db.value.ClobBase;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueByte;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueDecimal;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueFloat;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueShort;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueTime;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.sql.SQLCommand;

/**
 * Represents a prepared statement.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    protected SQLCommand command;
    private ArrayList<Value[]> batchParameters;
    private HashMap<String, Integer> cachedColumnLabelMap;

    JdbcPreparedStatement(JdbcConnection conn, String sql, int id, int resultSetType,
            int resultSetConcurrency) {
        this(conn, sql, id, resultSetType, resultSetConcurrency, false);
    }

    JdbcPreparedStatement(JdbcConnection conn, String sql, int id, int resultSetType,
            int resultSetConcurrency, boolean closedByResultSet) {
        super(conn, id, resultSetType, resultSetConcurrency, closedByResultSet);
        trace = conn.getTrace(TraceObjectType.PREPARED_STATEMENT, id);
        command = conn.prepareSQLCommand(sql, fetchSize);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        super.setFetchSize(rows);
        // 不能直接用rows，如果是0在调用父类的setFetchSize后会自动变成默认值
        command.setFetchSize(getFetchSize());
    }

    /**
     * Cache the column labels (looking up the column index can sometimes show
     * up on the performance profile).
     *
     * @param cachedColumnLabelMap the column map
     */
    void setCachedColumnLabelMap(HashMap<String, Integer> cachedColumnLabelMap) {
        this.cachedColumnLabelMap = cachedColumnLabelMap;
    }

    /**
     * Executes a query (select statement) and returns the result set. If
     * another result set exists for this statement, this will be closed (even
     * if this statement fails).
     *
     * @return the result set
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            return executeQueryInternal(true).get();
        } catch (Exception e) {
            throw logAndConvert(e); // 抛出SQLException
        }
    }

    public Future<ResultSet> executeQueryAsync() {
        try {
            return executeQueryInternal(false);
        } catch (Exception e) {
            return Future.failedFuture(logAndConvert(e)); // 返回失败Future
        }
    }

    private Future<ResultSet> executeQueryInternal(boolean sync) throws SQLException {
        int id = getNextTraceId(TraceObjectType.RESULT_SET);
        if (isDebugEnabled()) {
            debugCodeAssign(TraceObjectType.RESULT_SET, id,
                    sync ? "executeQuery()" : "executeQueryAsync()");
        }
        checkClosed();
        closeOldResultSet();
        setExecutingStatement(command);
        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
        AsyncCallback<ResultSet> ac = new AsyncCallback<>();
        command.executeQuery(maxRows, scrollable).onComplete(ar -> {
            setExecutingStatement(null);
            if (ar.isSucceeded()) {
                Result r = ar.getResult();
                boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                JdbcResultSet resultSet = new JdbcResultSet(conn, JdbcPreparedStatement.this, r, id,
                        closedByResultSet, scrollable, updatable, cachedColumnLabelMap);
                // resultSet.setCommand(command); //不能这样做，command是被重用的
                ac.setAsyncResult(resultSet);
            } else {
                // 转换成SQLException
                ac.setAsyncResult(DbException.toSQLException(ar.getCause()));
            }
        });
        return ac;
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            debugCodeCall("executeQuery", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @return the update count (number of row affected by an insert, update or
     *         delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback)
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public int executeUpdate() throws SQLException {
        debugCodeCall("executeUpdate");
        try {
            return executeUpdateInternal().get();
        } catch (Exception e) {
            throw logAndConvert(e); // 抛出SQLException
        }
    }

    public Future<Integer> executeUpdateAsync() {
        debugCodeCall("executeUpdateAsync");
        try {
            return executeUpdateInternal();
        } catch (Exception e) {
            return Future.failedFuture(logAndConvert(e)); // 返回失败Future
        }
    }

    private Future<Integer> executeUpdateInternal() throws SQLException {
        checkClosed();
        closeOldResultSet();
        setExecutingStatement(command);
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        command.executeUpdate().onComplete(ar -> {
            setExecutingStatement(null);
            if (ar.isFailed()) {
                // 转换成SQLException
                ac.setAsyncResult(DbException.toSQLException(ar.getCause()));
            } else {
                ac.setAsyncResult(ar);
            }
        });
        return ac;
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param autoGeneratedKeys ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            debugCode("executeUpdate(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param columnIndexes ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            debugCode("executeUpdate(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param columnNames ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            debugCode("executeUpdate(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this
     * statement, this will be closed (even if this statement fails). If auto
     * commit is on, and the statement is not a select, this statement will be
     * committed.
     *
     * @return true if a result set is available, false if not
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public boolean execute() throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("execute");
            }
            checkClosed();
            closeOldResultSet();
            setExecutingStatement(command);
            if (command.isQuery()) {
                resultSet = executeQuerySQLCommand(command);
                return true;
            } else {
                updateCount = command.executeUpdate().get();
                return false;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            setExecutingStatement(null);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            debugCodeCall("execute", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param autoGeneratedKeys ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            debugCode("execute(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param columnIndexes ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            debugCode("execute(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @param columnNames ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            debugCode("execute(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     */
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosed();
            if (batchParameters == null || batchParameters.isEmpty())
                return new int[0];

            if (command instanceof ClientPreparedSQLCommand) {
                ArrayList<Value[]> parameters = batchParameters;
                batchParameters = null;
                return ((ClientPreparedSQLCommand) command).executeBatchPreparedSQLCommands(parameters);
            } else {
                int size = batchParameters.size();
                int[] result = new int[size];
                boolean error = false;
                SQLException next = null;

                for (int i = 0; i < size; i++) {
                    Value[] set = batchParameters.get(i);
                    List<? extends CommandParameter> parameters = command.getParameters();
                    for (int j = 0; j < set.length; j++) {
                        Value value = set[j];
                        CommandParameter param = parameters.get(j);
                        param.setValue(value, false);
                    }
                    try {
                        result[i] = executeUpdateInternal().get();
                    } catch (Exception re) {
                        SQLException e = logAndConvert(re);
                        if (next == null) {
                            next = e;
                        } else {
                            e.setNextException(next);
                            next = e;
                        }
                        result[i] = Statement.EXECUTE_FAILED;
                        error = true;
                    }
                }
                batchParameters = null;
                if (error) {
                    JdbcBatchUpdateException e = new JdbcBatchUpdateException(next, result);
                    throw e;
                }
                return result;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Adds the current settings to the batch.
     */
    @Override
    public void addBatch() throws SQLException {
        try {
            debugCodeCall("addBatch");
            checkClosed();
            List<? extends CommandParameter> parameters = command.getParameters();
            int size = parameters.size();
            Value[] set = new Value[size];
            for (int i = 0; i < size; i++) {
                CommandParameter param = parameters.get(i);
                Value value = param.getValue();
                set[i] = value;
            }
            if (batchParameters == null) {
                batchParameters = Utils.newSmallArrayList();
            }
            batchParameters.add(set);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    @Override
    public void clearBatch() throws SQLException {
        try {
            debugCodeCall("clearBatch");
            checkClosed();
            batchParameters = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all parameters.
     *
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public void clearParameters() throws SQLException {
        try {
            debugCodeCall("clearParameters");
            checkClosed();
            List<? extends CommandParameter> parameters = command.getParameters();
            for (int i = 0, size = parameters.size(); i < size; i++) {
                CommandParameter param = parameters.get(i);
                // can only delete old temp files if they are not in the batch
                param.setValue(null, batchParameters == null);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes this statement.
     * All result sets that where created by this statement
     * become invalid after calling this method.
     */
    @Override
    public void close() throws SQLException {
        try {
            setExecutingStatement(null);
            super.close();
            batchParameters = null;
            if (command != null) {
                command.close();
                command = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Sets a parameter to null.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ");");
            }
            setParameter(parameterIndex, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setInt(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + parameterIndex + ", " + quote(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBigDecimal(" + parameterIndex + ", " + quoteBigDecimal(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueDecimal.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueDate.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueTime.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueTimestamp.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x);");
            }
            if (x == null) {
                // throw Errors.getInvalidValueException("null", "x");
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DataType.convertToValue(session, x, Value.UNKNOWN));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ");");
            }
            int type = DataType.convertSQLTypeToValueType(targetSqlType);
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                Value v = DataType.convertToValue(conn.getSession(), x, type);
                setParameter(parameterIndex, v.convertTo(type));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @param scale is ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ", " + scale + ");");
            }
            setObject(parameterIndex, x, targetSqlType);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBoolean(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setByte(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setShort(" + parameterIndex + ", (short) " + x + ");");
            }
            setParameter(parameterIndex, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setLong(" + parameterIndex + ", " + x + "L);");
            }
            setParameter(parameterIndex, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setFloat(" + parameterIndex + ", " + x + "f);");
            }
            setParameter(parameterIndex, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDouble(" + parameterIndex + ", " + x + "d);");
            }
            setParameter(parameterIndex, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a column as a reference.
     */
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Sets the date using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    @Override
    public void setDate(int parameterIndex, java.sql.Date x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertDate(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the time using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    @Override
    public void setTime(int parameterIndex, java.sql.Time x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertTime(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the timestamp using a specified time zone. The value will be
     * converted to the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar calendar)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertTimestamp(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] This feature is deprecated and not supported.
     *
     * @deprecated since JDBC 2.0, use setCharacterStream
     */
    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * Sets a parameter to null.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @param typeName this parameter is ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ", " + quote(typeName) + ");");
            }
            setNull(parameterIndex, sqlType);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else if (x instanceof BlobBase) {
                v = ((BlobBase) x).getValue();
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v = conn.createBlob(x, -1);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else if (x instanceof ClobBase) {
                v = ((ClobBase) x).getValue();
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x, -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a Array.
     */
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setArray(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = DataType.convertToValue(session, x.getArray(), Value.ARRAY);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a byte array.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes(" + parameterIndex + ", " + quoteBytes(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueBytes.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAsciiStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setAsciiStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        setCharacterStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        setCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setCharacterStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw unsupported("url");
    }

    /**
     * Gets the result set metadata of the query returned when the statement is
     * executed. If this is not a query, this method returns null.
     *
     * @return the meta data or null if this is not a query
     * @throws SQLException if this object is closed
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            debugCodeCall("getMetaData");
            checkClosed();
            Result result = command.getMetaData();
            if (result == null) {
                return null;
            }
            int id = getNextTraceId(TraceObjectType.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            String catalog = conn.getCatalog();
            JdbcResultSetMetaData meta = new JdbcResultSetMetaData(conn, catalog, this, null, result,
                    id);
            return meta;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the parameter meta data of this prepared statement.
     *
     * @return the meta data
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.PARAMETER_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.PARAMETER_META_DATA, id, "getParameterMetaData()");
            }
            checkClosed();
            JdbcParameterMetaData meta = new JdbcParameterMetaData(this, command, id);
            return meta;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private void setParameter(int parameterIndex, Value value) {
        checkClosed();
        parameterIndex--;
        List<? extends CommandParameter> parameters = command.getParameters();
        if (parameterIndex < 0 || parameterIndex >= parameters.size()) {
            throw DbException.getInvalidValueException("parameterIndex", parameterIndex + 1);
        }
        CommandParameter param = parameters.get(parameterIndex);
        // can only delete old temp files if they are not in the batch
        param.setValue(value, batchParameters == null);
    }

    /**
     * [Not supported] Sets the value of a parameter as a row id.
     */
    // ## Java 1.6 ##
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNString(" + parameterIndex + ", " + quote(x) + ");");
            }
            Value v = x == null ? ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNCharacterStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        setNCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    // ## Java 1.6 ##
    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else if (x instanceof ClobBase) {
                v = ((ClobBase) x).getValue();
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v = conn.createClob(x, -1);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a SQLXML object.
     */
    // ## Java 1.6 ##
    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + command;
    }
}
