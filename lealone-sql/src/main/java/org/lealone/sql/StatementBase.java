/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.CommandParameter;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.SysProperties;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.router.SQLRouter;
import org.lealone.storage.PageKey;
import org.lealone.transaction.Transaction;

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
     * Whether to create a new object (for indexes).
     */
    protected boolean create = true;

    /**
     * The list of parameters.
     */
    protected ArrayList<Parameter> parameters;

    /**
     * If the query should be prepared before each execution. This is set for
     * queries with LIKE ?, because the query plan depends on the parameter
     * value.
     */
    protected boolean prepareAlways;

    private long modificationMetaId;
    private int objectId;
    private int currentRowNumber;
    private int rowScanCount;
    private boolean canReuse;
    private boolean local = true;
    private int fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
    private int statementId;

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
    public int getId() {
        return statementId;
    }

    @Override
    public void setId(int id) {
        statementId = id;
    }

    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public void setLocal(boolean local) {
        this.local = local;
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
    public abstract Result getMetaData();

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
        return prepareAlways || modificationMetaId < db.getModificationMetaId() || db.getSettings().recompileAlways;
    }

    /**
     * Get the meta data modification id of the database when this statement was
     * compiled.
     *
     * @return the meta data modification id
     */
    long getModificationMetaId() {
        return modificationMetaId;
    }

    /**
     * Set the meta data modification id of this statement.
     *
     * @param id the new id
     */
    void setModificationMetaId(long id) {
        this.modificationMetaId = id;
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
     * Get the parameter list.
     *
     * @return the parameter list
     */
    @Override
    public ArrayList<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Check if all parameters have been set.
     *
     * @throws DbException if any parameter has not been set
     */
    protected void checkParameters() {
        if (parameters != null) {
            for (int i = 0, size = parameters.size(); i < size; i++) {
                Parameter param = parameters.get(i);
                param.checkSet();
            }
        }
    }

    /**
     * Prepare this statement.
     */
    @Override
    public PreparedSQLStatement prepare() {
        // nothing to do
        return this;
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
     * Execute a query and return the result.
     * This method prepares everything and calls {@link #query(int)} finally.
     *
     * @param maxRows the maximum number of rows to return
     * @param scrollable if the result set must be scrollable (ignored)
     * @return the result set
     */
    @Override
    public Result query(int maxRows, boolean scrollable) {
        return query(maxRows);
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

    @Override
    public int update(String replicationName) {
        session.setReplicationName(replicationName);
        return update();
    }

    /**
     * Set the SQL statement.
     *
     * @param sql the SQL statement
     */
    public void setSQL(String sql) {
        this.sql = sql;
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
     * Get the SQL statement with the execution plan.
     *
     * @return the execution plan
     */
    public String getPlanSQL() {
        return null;
    }

    /**
     * Check if this statement was canceled.
     *
     * @throws DbException if it was canceled
     */
    @Override
    public void checkCanceled() {
        session.checkCanceled();
    }

    /**
     * Set the object id for this statement.
     *
     * @param i the object id
     */
    @Override
    public void setObjectId(int i) {
        this.objectId = i;
        this.create = false;
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
     * Print information about the statement executed if info trace level is enabled.
     *
     * @param startTimeNanos when the statement was started
     * @param rowCount the query or update row count
     */
    void trace(long startTimeNanos, int rowCount) {
        if (startTimeNanos > 0 && session.getTrace().isInfoEnabled()) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            String params = Trace.formatParams(getParameters());
            session.getTrace().infoSQL(getSQL(), params, rowCount, deltaTimeNanos / 1000 / 1000);
        }

        // startTimeNanos can be zero for the command that actually turns on statistics
        if (startTimeNanos > 0 && session.getDatabase().getQueryStatistics()) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            session.getDatabase().getQueryStatisticsData().update(getSQL(), deltaTimeNanos, rowCount);
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
     * Set the current row number.
     *
     * @param rowNumber the row number
     */
    protected boolean setCurrentRowNumber(int rowNumber) {
        boolean yieldIfNeeded = false;
        if ((++rowScanCount & 127) == 0) {
            checkCanceled();
            yieldIfNeeded = yieldIfNeeded();
        }
        this.currentRowNumber = rowNumber;
        setProgress();
        return yieldIfNeeded;
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
     * Notifies query progress via the DatabaseEventListener
     */
    private void setProgress() {
        if ((currentRowNumber & 127) == 0) {
            session.getDatabase().setProgress(DatabaseEventListener.STATE_STATEMENT_PROGRESS, sql, currentRowNumber, 0);
        }
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
     * Set the SQL statement of the exception to the given row.
     *
     * @param e the exception
     * @param rowId the row number
     * @param values the values of the row
     * @return the exception
     */
    protected DbException setRow(DbException e, int rowId, String values) {
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

    @Override
    public boolean isCacheable() {
        return false;
    }

    @Override
    public ServerSession getSession() {
        return session;
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

    @Override
    public void cancel() {
    }

    @Override
    public PreparedSQLStatement getWrappedStatement() {
        return this;
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

    protected double cost;

    @Override
    public double getCost() {
        return cost;
    }

    protected int priority = NORM_PRIORITY;

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        TableFilter tf = getTableFilter();
        if (tf != null)
            tf.setPageKeys(pageKeys);

        // 以同步的方式运行
        YieldableBase<Result> yieldable = createYieldableQuery(maxRows, scrollable, null);
        yieldable.setPageKeys(pageKeys);
        yieldable.run();
        return Future.succeededFuture(yieldable.getResult());
    }

    @Override
    public Future<Integer> executeUpdate(List<PageKey> pageKeys) {
        TableFilter tf = getTableFilter();
        if (tf != null)
            tf.setPageKeys(pageKeys);

        // 以同步的方式运行
        YieldableBase<Integer> yieldable = createYieldableUpdate(null);
        yieldable.setPageKeys(pageKeys);
        yieldable.run();
        return Future.succeededFuture(yieldable.getResult());
    }

    @Override
    public Future<ReplicationUpdateAck> executeReplicaUpdate(String replicationName) {
        Future<Integer> f = executeUpdate(null);
        ReplicationUpdateAck ack = (ReplicationUpdateAck) session.createReplicationUpdateAckPacket(f.get(), false);
        return Future.succeededFuture(ack);
    }

    @Override
    public boolean isDDL() {
        return false;
    }

    @Override
    public boolean isDatabaseStatement() {
        return false;
    }

    @Override
    public boolean isReplicationStatement() {
        return false;
    }

    @Override
    public void replicaCommit(long validKey, boolean autoCommit) {
        session.replicationCommit(validKey, autoCommit);
    }

    @Override
    public void replicaRollback() {
        session.rollback();
    }

    public TableFilter getTableFilter() {
        return null;
    }

    public Map<String, List<PageKey>> getNodeToPageKeyMap() {
        TableFilter tf = getTableFilter();
        if (tf != null)
            return tf.getNodeToPageKeyMap(session);
        return null;
    }

    public String getPlanSQL(boolean isDistributed) {
        return getSQL();
    }

    @Override
    public YieldableBase<Integer> createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new DefaultYieldableUpdate(this, asyncHandler);
    }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        return new DefaultYieldableQuery(this, maxRows, scrollable, asyncHandler);
    }

    public static abstract class YieldableBase<T> implements Yieldable<T> {

        private static enum State {
            start,
            execute,
            stop,
            stopped;
        }

        protected StatementBase statement;
        protected final ServerSession session;
        protected final Trace trace;
        protected final AsyncHandler<AsyncResult<T>> asyncHandler;
        protected final boolean async;
        protected AsyncResult<T> asyncResult;
        protected T result;
        protected long startTimeNanos;
        protected boolean isUpdate;
        protected boolean callStop = true;

        private State state = State.start;
        private int savepointId = 0;

        public YieldableBase(StatementBase statement, AsyncHandler<AsyncResult<T>> asyncHandler) {
            this.statement = statement;
            this.session = statement.getSession();
            this.trace = session.getTrace(TraceModuleType.COMMAND);
            this.asyncHandler = asyncHandler;
            this.async = asyncHandler != null;
        }

        // 子类通常只需要实现以下三个方法
        protected boolean startInternal() {
            return false;
        }

        protected void stopInternal() {
        }

        protected abstract boolean executeInternal();

        protected void setResult(T result, int rowCount) {
            this.result = result;
            if (result != null) {
                if (asyncHandler != null) {
                    asyncResult = new AsyncResult<>();
                    asyncResult.setResult(result);
                }
                statement.trace(startTimeNanos, rowCount);
                setProgress(DatabaseEventListener.STATE_STATEMENT_END);
            }
        }

        @Override
        public T getResult() {
            return result;
        }

        @Override
        public void setPageKeys(List<PageKey> pageKeys) {
            TableFilter tf = statement.getTableFilter();
            if (tf != null)
                tf.setPageKeys(pageKeys);
        }

        @Override
        public final boolean run() {
            switch (state) {
            case start:
                if (start()) {
                    return true;
                }
                state = State.execute;
            case execute:
                if (execute()) {
                    return true;
                }
                state = State.stop;
            case stop:
                if (callStop) {
                    stop();
                }
            }
            return false;
        }

        private boolean start() {
            if (session.isExclusiveMode())
                return true;
            if (session.getDatabase().getQueryStatistics() || trace.isInfoEnabled()) {
                startTimeNanos = System.nanoTime();
            }
            if (isUpdate)
                savepointId = session.getTransaction(statement).getSavepointId();
            else
                session.getTransaction(statement);
            session.setCurrentCommand(statement);

            recompileIfNeeded();
            setProgress(DatabaseEventListener.STATE_STATEMENT_START);
            statement.checkParameters();
            return startInternal();
        }

        private void recompileIfNeeded() {
            if (statement.needRecompile()) {
                statement.setModificationMetaId(0);
                String sql = statement.getSQL();
                ArrayList<Parameter> oldParams = statement.getParameters();
                statement = (StatementBase) session.parseStatement(sql);
                long mod = statement.getModificationMetaId();
                statement.setModificationMetaId(0);
                ArrayList<Parameter> newParams = statement.getParameters();
                for (int i = 0, size = newParams.size(); i < size; i++) {
                    Parameter old = oldParams.get(i);
                    if (old.isValueSet()) {
                        Value v = old.getValue(session);
                        Parameter p = newParams.get(i);
                        p.setValue(v);
                    }
                }
                statement.prepare();
                statement.setModificationMetaId(mod);
            }
        }

        private void setProgress(int state) {
            session.getDatabase().setProgress(state, statement.getSQL(), 0, 0);
        }

        private boolean execute() {
            Database database = session.getDatabase();
            try {
                database.checkPowerOff();
                try {
                    return executeInternal();
                } catch (DbException e) {
                    filterConcurrentUpdate(e);
                    return true;
                } catch (Throwable e) {
                    throw DbException.convert(e);
                }
            } catch (DbException e) {
                handleException(e);
                return false;
            }
        }

        private void filterConcurrentUpdate(DbException e) {
            if (e.getErrorCode() != ErrorCode.CONCURRENT_UPDATE_1) {
                throw e;
            }
        }

        protected void handleException(DbException e) {
            callStop = false;
            e = e.addSQL(statement.getSQL());
            SQLException s = e.getSQLException();
            Database database = session.getDatabase();
            database.exceptionThrown(s, statement.getSQL());
            if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                // there is a serious problem:
                // the transaction may be applied partially
                // in this case we need to panic:
                // close the database
                database.shutdownImmediately();
                throw e;
            }
            database.checkPowerOff();
            if (isUpdate) {
                if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                    session.rollback();
                } else {
                    session.rollbackTo(savepointId);
                }
            }
            if (asyncHandler != null) {
                asyncResult = new AsyncResult<>();
                asyncResult.setCause(e);
                asyncHandler.handle(asyncResult);
                asyncResult = null; // 不需要再回调了
                stop();
            } else {
                stop();
                throw e;
            }
        }

        protected void stop() {
            stopInternal();
            session.closeTemporaryResults();
            session.closeCurrentCommand();
            if (asyncResult != null) {
                // 在复制模式下不能自动提交
                if (session.isAutoCommit() && session.getReplicationName() == null) {
                    // 不阻塞当前线程，异步提交事务，等到事务日志写成功后再给客户端返回语句的执行结果
                    session.asyncCommit(() -> asyncHandler.handle(asyncResult));
                } else {
                    // 当前语句是在一个手动提交的事务中进行，提前给客户端返回语句的执行结果
                    asyncHandler.handle(asyncResult);
                }
            } else {
                if (session.isAutoCommit() && session.getReplicationName() == null) {
                    // 阻塞当前线程，可能需要等事务日志写完为止
                    session.commit();
                }
            }
            if (startTimeNanos > 0 && trace.isInfoEnabled()) {
                long timeMillis = (System.nanoTime() - startTimeNanos) / 1000 / 1000;
                // 如果一条sql的执行时间大于100毫秒，记下它
                if (timeMillis > Constants.SLOW_QUERY_LIMIT_MS) {
                    trace.info("slow query: {0} ms, sql: {1}", timeMillis, statement.getSQL());
                }
            }
            state = State.stopped;
        }
    }

    public static abstract class YieldableUpdateBase extends YieldableBase<Integer> {

        protected int affectedRows;

        public YieldableUpdateBase(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            isUpdate = true;
        }

        protected void setResult(Integer result) {
            affectedRows = result;
            super.setResult(result, affectedRows);
        }
    }

    public static abstract class YieldableListenableUpdateBase extends YieldableUpdateBase
            implements Transaction.Listener {

        protected final AtomicInteger pendingOperationCounter = new AtomicInteger();
        protected volatile RuntimeException pendingOperationException;
        protected boolean loopEnd;

        public YieldableListenableUpdateBase(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);

            // 执行操作时都是异步的，
            // 当所有异步操作完成时才能调用stop方法给客户端发回响应结果
            callStop = false;
        }

        @Override
        protected boolean executeInternal() {
            if (!loopEnd) {
                if (executeAndListen()) {
                    return true;
                }
            }
            if (loopEnd) {
                if (pendingOperationException != null)
                    throw pendingOperationException;
                if (pendingOperationCounter.get() <= 0) {
                    setResult(affectedRows);
                    callStop = true;
                    return false;
                }
            }
            return true;
        }

        protected abstract boolean executeAndListen();

        @Override
        public void beforeOperation() {
            pendingOperationCounter.incrementAndGet();
        }

        @Override
        public void operationUndo() {
            pendingOperationCounter.decrementAndGet();
        }

        @Override
        public void operationComplete() {
            pendingOperationCounter.decrementAndGet();
        }

        @Override
        public void setException(RuntimeException e) {
            pendingOperationException = e;
        }
    }

    public static abstract class YieldableQueryBase extends YieldableBase<Result> {

        protected final int maxRows;
        protected final boolean scrollable;

        public YieldableQueryBase(StatementBase statement, int maxRows, boolean scrollable,
                AsyncHandler<AsyncResult<Result>> asyncHandler) {
            super(statement, asyncHandler);
            this.maxRows = maxRows;
            this.scrollable = scrollable;
            isUpdate = false;
        }
    }

    private static class DefaultYieldableUpdate extends YieldableUpdateBase {

        private Boolean completed;

        public DefaultYieldableUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            callStop = false;
        }

        @Override
        protected boolean executeInternal() {
            // session.setLastScopeIdentity(ValueNull.INSTANCE);
            if (completed == null) {
                completed = false;
                SQLRouter.executeUpdate(statement, ar -> {
                    if (ar.isSucceeded()) {
                        if (ar.getResult() < 0) {
                            completed = null; // 需要重新执行

                            // 在复制模式下执行时，可以把结果返回给客户端做冲突检测
                            if (session.getReplicationName() != null && asyncHandler != null) {
                                asyncHandler.handle(new AsyncResult<>(-1));
                            }
                        } else {
                            completed = true;
                            setResult(ar.getResult());
                            stop();
                        }
                    } else {
                        completed = true;
                        DbException e = DbException.convert(ar.getCause());
                        handleException(e);
                    }
                });
            }
            return completed == null || !completed;
        }
    }

    private static class DefaultYieldableQuery extends YieldableQueryBase {

        private Boolean completed;

        public DefaultYieldableQuery(StatementBase statement, int maxRows, boolean scrollable,
                AsyncHandler<AsyncResult<Result>> asyncHandler) {
            super(statement, maxRows, scrollable, asyncHandler);
            callStop = false;
        }

        @Override
        protected boolean executeInternal() {
            if (completed == null) {
                completed = false;
                SQLRouter.executeQuery(statement, maxRows, scrollable, ar -> {
                    try {
                        if (ar.isSucceeded()) {
                            Result result = ar.getResult();
                            setResult(result, result.getRowCount());
                            stop();
                        } else {
                            DbException e = DbException.convert(ar.getCause());
                            handleException(e);
                        }
                    } finally {
                        completed = true;
                    }
                });
            }
            return !completed;
        }
    }
}
