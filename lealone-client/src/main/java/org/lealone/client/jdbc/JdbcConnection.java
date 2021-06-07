/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.common.util.JdbcUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.db.session.Session;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.SQLCommand;

/**
 * <p>
 * Represents a connection (session) to a database.
 * </p>
 * <p>
 * Thread safety: the connection is thread-safe, because access
 * is synchronized. However, for compatibility with other databases, a
 * connection should only be used in one thread at any time.
 * </p>
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcConnection extends JdbcWrapper implements Connection {

    private final CompareMode compareMode = CompareMode.getInstance(null, 0, false);
    private final String url;
    private final String user;

    private Session session;
    private SQLCommand commit, rollback;
    private SQLCommand getReadOnly, getGeneratedKeys;
    private SQLCommand setTIL, getTIL; // set/get transaction isolation level
    private SQLCommand setQueryTimeout, getQueryTimeout;

    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private int queryTimeoutCache = -1;
    private int savepointId;
    private String catalog;

    public JdbcConnection(String url, Properties info) throws SQLException {
        this(new ConnectionInfo(url, info));
    }

    public JdbcConnection(ConnectionInfo ci) throws SQLException {
        try {
            // this will return an embedded or server connection
            session = ci.getSessionFactory().createSession(ci).get();
            user = ci.getUserName();
            url = ci.getURL(); // 不含参数
            initTrace();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    public JdbcConnection(Session session, ConnectionInfo ci) {
        this.session = session;
        user = ci.getUserName();
        url = ci.getURL();
        initTrace();
    }

    private void initTrace() {
        trace = getTrace(TraceObjectType.CONNECTION);
        if (isInfoEnabled()) {
            String format = "Connection %s = DriverManager.getConnection(%s, %s, \"\");";
            infoCode(format, getTraceObjectName(), quote(url), quote(user));
        }
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(Session session, String user, String url) {
        this.session = session;
        this.user = user;
        this.url = url;
        trace = getTrace(TraceObjectType.CONNECTION);
    }

    Trace getTrace(TraceObjectType traceObjectType) {
        return session.getTrace(TraceModuleType.JDBC, traceObjectType);
    }

    Trace getTrace(TraceObjectType traceObjectType, int traceObjectId) {
        return session.getTrace(TraceModuleType.JDBC, traceObjectType, traceObjectId);
    }

    /**
     * Creates a new statement.
     *
     * @return the new statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public Statement createStatement() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.STATEMENT, id, "createStatement()");
            }
            checkClosed();
            return new JdbcStatement(this, id, ResultSet.TYPE_FORWARD_ONLY, Constants.DEFAULT_RESULT_SET_CONCURRENCY);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type and concurrency.
     *
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.STATEMENT, id,
                        "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type, concurrency, and
     * holdability.
     *
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.STATEMENT, id, "createStatement(" + resultSetType + ", "
                        + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a prepared statement with the specified result set type and
     * concurrency.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a prepared statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the prepared statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ", "
                        + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param columnIndexes ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param columnNames ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Prepare a statement that will automatically close when the result set is
     * closed. This method is used to retrieve database meta data.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, true);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the database meta data for this database.
     *
     * @return the database meta data
     * @throws SQLException if the connection is closed
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.DATABASE_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.DATABASE_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            return new JdbcDatabaseMetaData(this, trace, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public Session getSession() {
        return session;
    }

    /**
     * Closes this connection. All open statements, prepared statements and
     * result sets that where created by this connection become invalid after
     * calling this method. If there is an uncommitted transaction, it will be
     * rolled back.
     */
    @Override
    public synchronized void close() throws SQLException {
        try {
            debugCodeCall("close");
            if (session == null) {
                return;
            }
            session.cancel();
            synchronized (session) {
                try {
                    if (!session.isClosed()) {
                        try {
                            closePreparedCommands();
                        } finally {
                            session.close();
                        }
                    }
                } finally {
                    session = null;
                }
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void closePreparedCommands() {
        commit = closeAndSetNull(commit);
        rollback = closeAndSetNull(rollback);
        getReadOnly = closeAndSetNull(getReadOnly);
        getGeneratedKeys = closeAndSetNull(getGeneratedKeys);
        getTIL = closeAndSetNull(getTIL);
        setTIL = closeAndSetNull(setTIL);
        getQueryTimeout = closeAndSetNull(getQueryTimeout);
        setQueryTimeout = closeAndSetNull(setQueryTimeout);
    }

    private static SQLCommand closeAndSetNull(SQLCommand command) {
        if (command != null) {
            command.close();
        }
        return null;
    }

    /**
     * Switches auto commit on or off. Enabling it commits an uncommitted
     * transaction, if there is one.
     *
     * @param autoCommit true for auto commit on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAutoCommit(" + autoCommit + ");");
            }
            checkClosed();
            if (autoCommit && !session.isAutoCommit()) {
                commit();
            }
            session.setAutoCommit(autoCommit);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current setting for auto commit.
     *
     * @return true for on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized boolean getAutoCommit() throws SQLException {
        try {
            checkClosed();
            debugCodeCall("getAutoCommit");
            return session.isAutoCommit();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Commits the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void commit() throws SQLException {
        try {
            debugCodeCall("commit");
            checkClosed();
            commit = prepareSQLCommand("COMMIT", commit);
            commit.executeUpdate().get();
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            session.reconnectIfNeeded();
        }
    }

    /**
     * Rolls back the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void rollback() throws SQLException {
        try {
            debugCodeCall("rollback");
            checkClosed();
            rollbackInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            session.reconnectIfNeeded();
        }
    }

    /**
     * Returns true if this connection has been closed.
     *
     * @return true if close was called
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return session == null || session.isClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Translates a SQL statement into the database grammar.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @return the translated statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        try {
            debugCodeCall("nativeSQL", sql);
            checkClosed();
            return translateSQL(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * According to the JDBC specs, this setting is only a hint to the database
     * to enable optimizations - it does not cause writes to be prohibited.
     *
     * @param readOnly ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setReadOnly(" + readOnly + ");");
            }
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if the database is read-only.
     *
     * @return if the database is read-only
     * @throws SQLException if the connection is closed
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            debugCodeCall("isReadOnly");
            checkClosed();
            getReadOnly = prepareSQLCommand("CALL READONLY()", getReadOnly);
            Result result = getReadOnly.executeQuery(0, false).get();
            result.next();
            boolean readOnly = result.currentRow()[0].getBoolean();
            return readOnly;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Set the default catalog name. This call is ignored.
     *
     * @param catalog ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        try {
            debugCodeCall("setCatalog", catalog);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current catalog name.
     *
     * @return the catalog name
     * @throws SQLException if the connection is closed
     */
    @Override
    public String getCatalog() throws SQLException {
        try {
            debugCodeCall("getCatalog");
            checkClosed();
            if (catalog == null) {
                SQLCommand command = prepareSQLCommand("CALL DATABASE()", Integer.MAX_VALUE);
                Result result = command.executeQuery(0, false).get();
                result.next();
                catalog = result.currentRow()[0].getString();
                command.close();
            }
            return catalog;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings.
     */
    @Override
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit an open transaction, even if the new level is the same as the old
     * one, except if the level is not supported.
     *
     * @param level the new transaction isolation level:
     *            Connection.TRANSACTION_READ_UNCOMMITTED,
     *            Connection.TRANSACTION_READ_COMMITTED,
     *            Connection.TRANSACTION_REPEATABLE_READ, or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @throws SQLException if the connection is closed or the isolation level
     *             is not supported
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            debugCodeCall("setTransactionIsolation", level);
            checkClosed();
            switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                break;
            default:
                throw DbException.getInvalidValueException("level", level);
            }
            commit();
            setTIL = prepareSQLCommand("SET TRANSACTION_ISOLATION_LEVEL ?", setTIL);
            setTIL.getParameters().get(0).setValue(ValueInt.get(level), false);
            setTIL.executeUpdate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the current transaction isolation level.
     *
     * @return the isolation level.
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            debugCodeCall("getTransactionIsolation");
            checkClosed();
            getTIL = prepareSQLCommand("CALL TRANSACTION_ISOLATION_LEVEL()", getTIL);
            Result result = getTIL.executeQuery(0, false).get();
            result.next();
            int transactionIsolationLevel = result.currentRow()[0].getInt();
            result.close();
            return transactionIsolationLevel;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            setQueryTimeout = prepareSQLCommand("SET QUERY_TIMEOUT ?", setQueryTimeout);
            setQueryTimeout.getParameters().get(0).setValue(ValueInt.get(seconds * 1000), false);
            setQueryTimeout.executeUpdate();
            queryTimeoutCache = seconds;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public int getQueryTimeout() throws SQLException {
        try {
            debugCodeCall("getQueryTimeout");
            if (queryTimeoutCache == -1) {
                checkClosed();
                getQueryTimeout = prepareSQLCommand("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?",
                        getQueryTimeout);
                getQueryTimeout.getParameters().get(0).setValue(ValueString.get("QUERY_TIMEOUT"), false);
                Result result = getQueryTimeout.executeQuery(0, false).get();
                result.next();
                int queryTimeout = result.currentRow()[0].getInt();
                result.close();
                if (queryTimeout != 0) {
                    // round to the next second, otherwise 999 millis would return 0 seconds
                    queryTimeout = (queryTimeout + 999) / 1000;
                }
                queryTimeoutCache = queryTimeout;
            }
            return queryTimeoutCache;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current result set holdability.
     *
     * @param holdability
     *            ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *            ResultSet.CLOSE_CURSORS_AT_COMMIT;
     * @throws SQLException
     *            if the connection is closed or the holdability is not
     *            supported
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        try {
            debugCodeCall("setHoldability", holdability);
            checkClosed();
            checkHoldability(holdability);
            this.holdability = holdability;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {
        try {
            debugCodeCall("getHoldability");
            checkClosed();
            return holdability;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the type map.
     *
     * @return null
     * @throws SQLException if the connection is closed
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            debugCodeCall("getTypeMap");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Partially supported] Sets the type map. This is only supported if the
     * map is empty or null.
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        try {
            debugCode("setTypeMap(" + quoteMap(map) + ");");
            checkMap(map);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new callable statement.
     *
     * @param sql the SQL statement
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the statement is not valid
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type and
     * concurrency.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.CALLABLE_STATEMENT, id,
                        "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ", "
                        + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new unnamed savepoint.
     *
     * @return the new savepoint
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.SAVEPOINT, id, "setSavepoint()");
            }
            checkClosed();
            SQLCommand set = prepareSQLCommand("SAVEPOINT " + JdbcSavepoint.getName(null, savepointId),
                    Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null, trace, id);
            savepointId++;
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new named savepoint.
     *
     * @param name the savepoint name
     * @return the new savepoint
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign(TraceObjectType.SAVEPOINT, id, "setSavepoint(" + quote(name) + ")");
            }
            checkClosed();
            SQLCommand set = prepareSQLCommand("SAVEPOINT " + JdbcSavepoint.getName(name, 0), Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, 0, name, trace, id);
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Rolls back to a savepoint.
     *
     * @param savepoint the savepoint
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        try {
            JdbcSavepoint sp = convertSavepoint(savepoint);
            debugCode("rollback(" + sp.getTraceObjectName() + ");");
            checkClosed();
            sp.rollback();
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            session.reconnectIfNeeded();
        }
    }

    /**
     * Releases a savepoint.
     *
     * @param savepoint the savepoint to release
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            debugCode("releaseSavepoint(savepoint);");
            checkClosed();
            convertSavepoint(savepoint).release();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private static JdbcSavepoint convertSavepoint(Savepoint savepoint) {
        if (!(savepoint instanceof JdbcSavepoint)) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, "" + savepoint);
        }
        return (JdbcSavepoint) savepoint;
    }

    // =============================================================

    SQLCommand createSQLCommand(String sql, int fetchSize) {
        return session.createSQLCommand(sql, fetchSize);
    }

    /**
     * Prepare an command. This will parse the SQL statement.
     *
     * @param sql the SQL statement
     * @param fetchSize the fetch size (used in remote connections)
     * @return the command
     */
    SQLCommand prepareSQLCommand(String sql, int fetchSize) {
        return session.prepareSQLCommand(sql, fetchSize);
    }

    private SQLCommand prepareSQLCommand(String sql, SQLCommand old) {
        return old == null ? prepareSQLCommand(sql, Integer.MAX_VALUE) : old;
    }

    private static int translateGetEnd(String sql, int i, char c) {
        int len = sql.length();
        switch (c) {
        case '$': {
            if (i < len - 1 && sql.charAt(i + 1) == '$' && (i == 0 || sql.charAt(i - 1) <= ' ')) {
                int j = sql.indexOf("$$", i + 2);
                if (j < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                return j + 1;
            }
            return i;
        }
        case '\'': {
            int j = sql.indexOf('\'', i + 1);
            if (j < 0) {
                throw DbException.getSyntaxError(sql, i);
            }
            return j;
        }
        case '"': {
            int j = sql.indexOf('"', i + 1);
            if (j < 0) {
                throw DbException.getSyntaxError(sql, i);
            }
            return j;
        }
        case '/': {
            checkRunOver(i + 1, len, sql);
            if (sql.charAt(i + 1) == '*') {
                // block comment
                int j = sql.indexOf("*/", i + 2);
                if (j < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                i = j + 1;
            } else if (sql.charAt(i + 1) == '/') {
                // single line comment
                i += 2;
                while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                    i++;
                }
            }
            return i;
        }
        case '-': {
            checkRunOver(i + 1, len, sql);
            if (sql.charAt(i + 1) == '-') {
                // single line comment
                i += 2;
                while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                    i++;
                }
            }
            return i;
        }
        default:
            throw DbException.getInternalError("c=" + c);
        }
    }

    /**
     * Convert JDBC escape sequences in the SQL statement. This
     * method throws an exception if the SQL statement is null.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @return the SQL statement without JDBC escape sequences
     */
    private static String translateSQL(String sql) {
        return translateSQL(sql, true);
    }

    /**
     * Convert JDBC escape sequences in the SQL statement if required. This
     * method throws an exception if the SQL statement is null.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @param escapeProcessing whether escape sequences should be replaced
     * @return the SQL statement without JDBC escape sequences
     */
    static String translateSQL(String sql, boolean escapeProcessing) {
        if (sql == null) {
            throw DbException.getInvalidValueException("SQL", null);
        }
        if (!escapeProcessing) {
            return sql;
        }
        if (sql.indexOf('{') < 0) {
            return sql;
        }
        int len = sql.length();
        char[] chars = null;
        int level = 0;
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            switch (c) {
            case '\'':
            case '"':
            case '/':
            case '-':
                i = translateGetEnd(sql, i, c);
                break;
            case '{':
                level++;
                if (chars == null) {
                    chars = sql.toCharArray();
                }
                chars[i] = ' ';
                while (Character.isSpaceChar(chars[i])) {
                    i++;
                    checkRunOver(i, len, sql);
                }
                int start = i;
                if (chars[i] >= '0' && chars[i] <= '9') {
                    chars[i - 1] = '{';
                    while (true) {
                        checkRunOver(i, len, sql);
                        c = chars[i];
                        if (c == '}') {
                            break;
                        }
                        switch (c) {
                        case '\'':
                        case '"':
                        case '/':
                        case '-':
                            i = translateGetEnd(sql, i, c);
                            break;
                        default:
                        }
                        i++;
                    }
                    level--;
                    break;
                } else if (chars[i] == '?') {
                    i++;
                    checkRunOver(i, len, sql);
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                    if (sql.charAt(i) != '=') {
                        throw DbException.getSyntaxError(sql, i, "=");
                    }
                    i++;
                    checkRunOver(i, len, sql);
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                }
                while (!Character.isSpaceChar(chars[i])) {
                    i++;
                    checkRunOver(i, len, sql);
                }
                int remove = 0;
                if (found(sql, start, "fn")) {
                    remove = 2;
                } else if (found(sql, start, "escape")) {
                    break;
                } else if (found(sql, start, "call")) {
                    break;
                } else if (found(sql, start, "oj")) {
                    remove = 2;
                } else if (found(sql, start, "ts")) {
                    remove = 2;
                } else if (found(sql, start, "t")) {
                    remove = 1;
                } else if (found(sql, start, "d")) {
                    remove = 1;
                } else if (found(sql, start, "params")) {
                    remove = "params".length();
                }
                for (i = start; remove > 0; i++, remove--) {
                    chars[i] = ' ';
                }

                // 对于PARAMETERS('replication_strategy':'SimpleStrategy')会有bug，漏掉(后的单引号
                if (sql.charAt(i) == '\'')
                    i--;
                break;
            case '}':
                if (--level < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                chars[i] = ' ';
                break;
            case '$':
                i = translateGetEnd(sql, i, c);
                break;
            default:
            }
        }
        if (level != 0) {
            throw DbException.getSyntaxError(sql, sql.length() - 1);
        }
        if (chars != null) {
            sql = new String(chars);
        }
        return sql;
    }

    private static void checkRunOver(int i, int len, String sql) {
        if (i >= len) {
            throw DbException.getSyntaxError(sql, i);
        }
    }

    private static boolean found(String sql, int start, String other) {
        return sql.regionMatches(true, start, other, 0, other.length());
    }

    private static void checkTypeConcurrency(int resultSetType, int resultSetConcurrency) {
        switch (resultSetType) {
        case ResultSet.TYPE_FORWARD_ONLY:
        case ResultSet.TYPE_SCROLL_INSENSITIVE:
        case ResultSet.TYPE_SCROLL_SENSITIVE:
            break;
        default:
            throw DbException.getInvalidValueException("resultSetType", resultSetType);
        }
        switch (resultSetConcurrency) {
        case ResultSet.CONCUR_READ_ONLY:
        case ResultSet.CONCUR_UPDATABLE:
            break;
        default:
            throw DbException.getInvalidValueException("resultSetConcurrency", resultSetConcurrency);
        }
    }

    private static void checkHoldability(int resultSetHoldability) {
        // TODO compatibility / correctness: DBPool uses
        // ResultSet.HOLD_CURSORS_OVER_COMMIT
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT
                && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw DbException.getInvalidValueException("resultSetHoldability", resultSetHoldability);
        }
    }

    /**
     * INTERNAL.
     * Check if this connection is closed.
     * 
     * @throws DbException if the connection or session is closed
     */
    protected void checkClosed() {
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
    }

    String getURL() {
        checkClosed();
        return url;
    }

    String getUser() {
        checkClosed();
        return user;
    }

    private void rollbackInternal() {
        rollback = prepareSQLCommand("ROLLBACK", rollback);
        rollback.executeUpdate();
    }

    /**
     * INTERNAL
     */
    ResultSet getGeneratedKeys(JdbcStatement stat, int id) {
        getGeneratedKeys = prepareSQLCommand("SELECT SCOPE_IDENTITY() WHERE SCOPE_IDENTITY() IS NOT NULL",
                getGeneratedKeys);
        Result result = getGeneratedKeys.executeQuery(0, false).get();
        ResultSet rs = new JdbcResultSet(this, stat, result, id, false, true, false);
        return rs;
    }

    /**
     * Create a new empty Clob object.
     *
     * @return the object
     */
    @Override
    public Clob createClob() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.CLOB);
            debugCodeAssign(TraceObjectType.CLOB, id, "createClob()");
            checkClosed();
            Value v = session.getDataHandler().getLobStorage()
                    .createClob(new InputStreamReader(new ByteArrayInputStream(Utils.EMPTY_BYTES)), 0);
            return new JdbcClob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty Blob object.
     *
     * @return the object
     */
    @Override
    public Blob createBlob() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.BLOB);
            debugCodeAssign(TraceObjectType.BLOB, id, "createClob()");
            checkClosed();
            Value v = session.getDataHandler().getLobStorage().createBlob(new ByteArrayInputStream(Utils.EMPTY_BYTES),
                    0);
            return new JdbcBlob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty NClob object.
     *
     * @return the object
     */
    // ## Java 1.6 ##
    @Override
    public NClob createNClob() throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.CLOB);
            debugCodeAssign("NClob", TraceObjectType.CLOB, id, "createNClob()");
            checkClosed();
            Value v = session.getDataHandler().getLobStorage()
                    .createClob(new InputStreamReader(new ByteArrayInputStream(Utils.EMPTY_BYTES)), 0);
            return new JdbcClob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Create a new empty SQLXML object.
     */
    // ## Java 1.6 ##
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * Create a new Array object.
     *
     * @param typeName the type name
     * @param elements the values
     * @return the array
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            int id = getNextTraceId(TraceObjectType.ARRAY);
            debugCodeAssign(TraceObjectType.ARRAY, id, "createArrayOf()");
            checkClosed();
            Value value = DataType.convertToValue(session, elements, Value.ARRAY);
            return new JdbcArray(this, value, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Create a new empty Struct object.
     */
    // ## Java 1.6 ##
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw unsupported("Struct");
    }

    /**
     * Returns true if this connection is still valid.
     *
     * @param timeout the number of seconds to wait for the database to respond
     *            (ignored)
     * @return true if the connection is valid.
     */
    @Override
    public synchronized boolean isValid(int timeout) {
        try {
            debugCodeCall("isValid", timeout);
            if (session == null || session.isClosed()) {
                return false;
            }
            // force a network round trip (if networked)
            getTransactionIsolation();
            return true;
        } catch (Exception e) {
            // this method doesn't throw an exception, but it logs it
            logAndConvert(e);
            return false;
        }
    }

    /**
     * [Not supported] Set a client property.
     */
    // ## Java 1.6 ##
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Set the client properties.
     */
    // ## Java 1.6 ##
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Get the client properties.
     */
    // ## Java 1.6 ##
    @Override
    public Properties getClientInfo() throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Set a client property.
     */
    // ## Java 1.6 ##
    @Override
    public String getClientInfo(String name) throws SQLException {
        throw unsupported("clientInfo");
    }

    /**
     * Create a Clob value from this reader.
     *
     * @param x the reader
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    public Value createClob(Reader x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = session.getDataHandler().getLobStorage().createClob(x, length);
        return v;
    }

    /**
     * Create a Blob value from this input stream.
     *
     * @param x the input stream
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    public Value createBlob(InputStream x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = session.getDataHandler().getLobStorage().createBlob(x, length);
        return v;
    }

    private static void checkMap(Map<String, Class<?>> map) {
        if (map != null && map.size() > 0) {
            throw DbException.getUnsupportedException("map.size > 0");
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }

    /**
     * Convert an object to the default Java object for the given SQL type. For
     * example, LOB objects are converted to java.sql.Clob / java.sql.Blob.
     *
     * @param v the value
     * @return the object
     */
    Object convertToDefaultObject(Value v) {
        Object o;
        switch (v.getType()) {
        case Value.CLOB: {
            int id = getNextTraceId(TraceObjectType.CLOB);
            o = new JdbcClob(this, v, id);
            break;
        }
        case Value.BLOB: {
            int id = getNextTraceId(TraceObjectType.BLOB);
            o = new JdbcBlob(this, v, id);
            break;
        }
        case Value.JAVA_OBJECT:
            if (SysProperties.SERIALIZE_JAVA_OBJECT) {
                o = Utils.deserialize(v.getBytesNoCopy());
                break;
            }
        default:
            o = v.getObject();
        }
        return o;
    }

    CompareMode getCompareMode() {
        return compareMode;
    }

    // ## Java 1.7 ##
    @Override
    public void setSchema(String schema) throws SQLException {
        Statement stmt = null;
        try {
            debugCodeCall("setSchema", schema);
            checkClosed();
            stmt = createStatement();
            stmt.executeUpdate("USE " + schema);
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            JdbcUtils.closeSilently(stmt);
        }
    }

    // ## Java 1.7 ##
    @Override
    public String getSchema() throws SQLException {
        Statement stmt = null;
        try {
            debugCodeCall("getSchema");
            checkClosed();
            stmt = createStatement();
            ResultSet rs = stmt.executeQuery("CALL SCHEMA()");
            rs.next();
            String currentSchemaName = rs.getString(1);
            rs.close();
            return currentSchemaName;
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            JdbcUtils.closeSilently(stmt);
        }
    }

    // ## Java 1.7 ##
    @Override
    public void abort(Executor executor) throws SQLException {
        throw unsupported("abort(Executor)");
    }

    // ## Java 1.7 ##
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        try {
            debugCodeCall("setNetworkTimeout", milliseconds);
            checkClosed();
            if (milliseconds < 0)
                throw DbException.getInvalidValueException("milliseconds", milliseconds);
            session.setNetworkTimeout(milliseconds);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // ## Java 1.7 ##
    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            debugCodeCall("getNetworkTimeout");
            return session.getNetworkTimeout();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
}
