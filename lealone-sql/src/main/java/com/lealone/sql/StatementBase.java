/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.Trace;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.CommandParameter;
import com.lealone.db.Database;
import com.lealone.db.SysProperties;
import com.lealone.db.api.DatabaseEventListener;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.SessionStatus;
import com.lealone.db.value.Value;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.executor.YieldableLocalUpdate;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.query.YieldableLocalQuery;

/**
 * A parsed and prepared statement.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class StatementBase implements PreparedSQLStatement, ParsedSQLStatement {

    /**
     * The session.
     */
    protected ServerSession session;

    /**
     * The SQL string.
     */
    protected String sql;

    /**
     * The list of parameters.
     */
    protected ArrayList<Parameter> parameters;

    /**
     * If the query should be prepared before each execution. This is set for
     * queries with LIKE ?, because the query plan depends on the parameter value.
     */
    protected boolean prepareAlways;

    protected int priority = NORM_PRIORITY;

    private int statementId;
    private long modificationMetaId;
    private int objectId;
    private int currentRowNumber;
    private int rowScanCount;
    private boolean canReuse;
    private int fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;

    /**
     * Create a new object.
     *
     * @param session the session
     */
    public StatementBase(ServerSession session) {
        this.session = session;
        modificationMetaId = session.getDatabase().getModificationMetaId();
    }

    @Override
    public ServerSession getSession() {
        return session;
    }

    /**
     * Set the session for this statement.
     *
     * @param currentSession the new session
     */
    public void setSession(ServerSession currentSession) {
        this.session = currentSession;
    }

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    @Override
    public String getSQL() {
        return sql;
    }

    /**
     * Set the SQL statement.
     *
     * @param sql the SQL statement
     */
    public void setSQL(String sql) {
        this.sql = sql;
    }

    @Override
    public int getId() {
        return statementId;
    }

    @Override
    public void setId(int id) {
        statementId = id;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        if (fetchSize < 0) {
            throw DbException.getInvalidValueException("fetchSize", fetchSize);
        }
        if (fetchSize == 0) {
            fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
        }
        this.fetchSize = fetchSize;
    }

    @Override
    public Future<Result> getMetaData() {
        return Future.succeededFuture(null);
    }

    /**
     * Get the statement type as defined in SQLStatement
     *
     * @return the statement type
     */
    @Override
    public abstract int getType();

    /**
     * Check if the statement needs to be re-compiled.
     *
     * @return true if it must
     */
    public boolean needRecompile() {
        Database db = session.getDatabase();
        if (db == null) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "database closed");
        }
        // parser: currently, compiling every create/drop/... twice
        // because needRecompile return true even for the first execution
        return prepareAlways || modificationMetaId < db.getModificationMetaId()
                || db.getSettings().recompileAlways;
    }

    /**
     * Get the meta data modification id of the database when this statement was
     * compiled.
     *
     * @return the meta data modification id
     */
    public long getModificationMetaId() {
        return modificationMetaId;
    }

    /**
     * Set the meta data modification id of this statement.
     *
     * @param id the new id
     */
    public void setModificationMetaId(long id) {
        this.modificationMetaId = id;
    }

    /**
     * Get the parameter list.
     *
     * @return the parameter list
     */
    @Override
    public ArrayList<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Set the parameter list of this statement.
     *
     * @param parameters the parameter list
     */
    public void setParameterList(ArrayList<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Check if all parameters have been set.
     *
     * @throws DbException if any parameter has not been set
     */
    public void checkParameters() {
        if (parameters != null) {
            for (int i = 0, size = parameters.size(); i < size; i++) {
                Parameter param = parameters.get(i);
                param.checkSet();
            }
        }
    }

    /**
     * Set the object id for this statement.
     *
     * @param objectId the object id
     */
    @Override
    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    /**
     * Get the object id to use for the database object that is created in this
     * statement. This id is only set when the object is persistent.
     * If not set, this method returns 0.
     *
     * @return the object id or 0 if not set
     */
    protected int getCurrentObjectId() {
        return objectId;
    }

    /**
     * Get the current object id, or get a new id from the database. The object
     * id is used when creating new database object (CREATE statement).
     *
     * @return the object id
     */
    protected int getObjectId() {
        return getObjectId(session.getDatabase());
    }

    protected int getObjectId(Database db) {
        int id = objectId;
        if (id == 0) {
            id = db.allocateObjectId();
        } else {
            objectId = 0;
        }
        return id;
    }

    /**
     * Convert the statement to a String.
     *
     * @return the SQL statement
     */
    @Override
    public String toString() {
        return sql;
    }

    /**
     * Get the SQL statement with the execution plan.
     *
     * @return the execution plan
     */
    public String getPlanSQL() {
        return getSQL();
    }

    /**
     * Get the SQL snippet of the value list.
     *
     * @param values the value list
     * @return the SQL snippet
     */
    protected static String getSQL(Value[] values) {
        StatementBuilder buff = new StatementBuilder();
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            if (v != null) {
                buff.append(v.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Get the SQL snippet of the expression list.
     *
     * @param list the expression list
     * @return the SQL snippet
     */
    protected static String getSQL(Expression[] list) {
        StatementBuilder buff = new StatementBuilder();
        for (Expression e : list) {
            buff.appendExceptFirst(", ");
            if (e != null) {
                buff.append(e.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Check if this statement was canceled.
     *
     * @throws DbException if it was canceled
     */
    public void checkCanceled() {
        session.checkCanceled();
    }

    @Override
    public void cancel() {
    }

    /**
     * Print information about the statement executed if info trace level is enabled.
     *
     * @param startTimeNanos when the statement was started
     * @param rowCount the query or update row count
     */
    public void trace(long startTimeNanos, int rowCount) {
        // startTimeNanos can be zero for the command that actually turns on statistics
        if (startTimeNanos > 0) {
            long now = System.nanoTime();
            long deltaTimeNanos = now - startTimeNanos;
            if (session.getTrace().isInfoEnabled()) {
                String params = Trace.formatParams(getParameters());
                session.getTrace().infoSQL(getSQL(), params, rowCount, deltaTimeNanos / 1000 / 1000);
            }
            Database db = session.getDatabase();
            if (db.getQueryStatistics()) {
                db.getQueryStatisticsData().update(getSQL(), deltaTimeNanos, rowCount);
            }
        }
    }

    /**
     * Set the prepare always flag.
     * If set, the statement is re-compiled whenever it is executed.
     *
     * @param prepareAlways the new value
     */
    public void setPrepareAlways(boolean prepareAlways) {
        this.prepareAlways = prepareAlways;
    }

    /**
     * Get the current row number.
     *
     * @return the row number
     */
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    /**
     * Set the current row number.
     *
     * @param rowNumber the row number
     */
    public boolean setCurrentRowNumber(int rowNumber) {
        return setCurrentRowNumber(rowNumber, true);
    }

    public boolean setCurrentRowNumber(int rowNumber, boolean yieldEnabled) {
        this.currentRowNumber = rowNumber;
        if ((++rowScanCount & 127) == 0) {
            checkCanceled();
            setProgress();
            if (yieldEnabled && session.getScheduler() != null)
                return session.getScheduler().yieldIfNeeded(this);
        }
        return false;
    }

    /**
     * Notifies query progress via the DatabaseEventListener
     */
    private void setProgress() {
        session.getDatabase().setProgress(DatabaseEventListener.STATE_STATEMENT_PROGRESS, sql,
                currentRowNumber, 0);
    }

    /**
     * Set the SQL statement of the exception to the given row.
     *
     * @param e the exception
     * @param rowId the row number
     * @param values the values of the row
     * @return the exception
     */
    public DbException setRow(DbException e, int rowId, String values) {
        StringBuilder buff = new StringBuilder();
        if (sql != null) {
            buff.append(sql);
        }
        buff.append(" -- ");
        if (rowId > 0) {
            buff.append("row #").append(rowId + 1).append(' ');
        }
        buff.append('(').append(values).append(')');
        return e.addSQL(buff.toString());
    }

    /**
     * Whether the statement is already closed (in which case it can be re-used).
     *
     * @return true if it can be re-used
     */
    @Override
    public boolean canReuse() {
        return canReuse;
    }

    /**
     * The statement is now re-used, therefore reset the canReuse flag, and the
     * parameter values.
     */
    @Override
    public void reuse() {
        canReuse = false;
        ArrayList<? extends CommandParameter> parameters = getParameters();
        for (int i = 0, size = parameters.size(); i < size; i++) {
            CommandParameter param = parameters.get(i);
            param.setValue(null, true);
        }
    }

    @Override
    public void close() {
        canReuse = true;
    }

    /**
     * Prepare this statement.
     */
    @Override
    public PreparedSQLStatement prepare() {
        // nothing to do
        return this;
    }

    @Override
    public Future<Boolean> prepare(boolean readParams) {
        prepare();
        return Future.succeededFuture(isQuery());
    }

    /**
     * Check if this object is a query.
     *
     * @return true if it is
     */
    @Override
    public boolean isQuery() {
        return false;
    }

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    @Override
    public Result query(int maxRows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    @Override
    public int update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    // 以同步的方式运行
    protected <K> K syncExecute(YieldableBase<K> yieldable) {
        yieldable.disableYield();
        while (!yieldable.isStopped()) {
            yieldable.run();
            // 如果在存储引擎层面没有顺利结束，需要执行其他语句
            if (session.getScheduler() != null && !yieldable.isStopped())
                session.getScheduler().executeNextStatement();
            while (session.getStatus() == SessionStatus.WAITING) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
        return yieldable.getResult();
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable) {
        // checkScheduler();
        AsyncCallback<Result> ac = session.createCallback();
        YieldableBase<Result> yieldable = createYieldableQuery(maxRows, scrollable, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                ac.setAsyncResult(result);
            } else {
                ac.setAsyncResult(ar.getCause());
            }
        });
        YieldableCommand c = new YieldableCommand(-1, yieldable, -1);
        session.setYieldableCommand(c);
        session.getScheduler().wakeUp();
        return ac;
    }

    @Override
    public Future<Integer> executeUpdate() {
        return executeUpdate(null);
    }

    @Override
    public Future<Integer> executeUpdate(Value[] parameterValues) {
        // checkScheduler();
        AsyncCallback<Integer> ac = session.createCallback();
        YieldableBase<Integer> yieldable = createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                Integer updateCount = ar.getResult();
                ac.setAsyncResult(updateCount);
            } else {
                ac.setAsyncResult(ar.getCause());
            }
        });
        YieldableCommand c = new YieldableCommand(-1, yieldable, -1);
        if (parameterValues != null)
            c.setParameterValues(parameterValues);
        session.setYieldableCommand(c);
        session.getScheduler().wakeUp();
        return ac;
    }

    // private void checkScheduler() {
    // if (session.getScheduler() == null
    // || SchedulerThread.currentScheduler() != session.getScheduler())
    // throw DbException.getInternalError();
    // }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        return new YieldableLocalQuery(this, maxRows, scrollable, asyncHandler);
    }

    @Override
    public YieldableBase<Integer> createYieldableUpdate(
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableLocalUpdate(this, asyncHandler);
    }
}
