/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql;

import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.api.DatabaseEventListener;
import org.lealone.api.ErrorCode;
import org.lealone.async.AsyncHandler;
import org.lealone.async.AsyncResult;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.MathUtils;
import org.lealone.db.CommandUpdateResult;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.db.table.StandardTable;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.router.RouterHolder;

/**
 * Represents a SQL statement wrapper.
 * 
 * @author H2 Group
 * @author zhh
 */
class StatementWrapper extends StatementBase {

    StatementBase statement;

    /**
     * The trace module.
     */
    private final Trace trace;

    /**
     * The last start time.
     */
    private long startTimeNanos;

    /**
     * If this query was canceled.
     */
    private volatile boolean cancel;

    StatementWrapper(ServerSession session, StatementBase statement) {
        super(session);
        this.statement = statement;
        trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    @Override
    public Result getMetaData() {
        return statement.getMetaData();
    }

    /**
     * Check if this command has been canceled, and throw an exception if yes.
     *
     * @throws DbException if the statement has been canceled
     */
    @Override
    public void checkCanceled() {
        if (cancel) {
            cancel = false;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    @Override
    public void close() {
        statement.close();
    }

    @Override
    public void cancel() {
        this.cancel = true;
        statement.cancel();
    }

    @Override
    public String toString() {
        return "StatementWrapper[" + statement.toString() + "]";
    }

    /**
     * Whether the command is already closed (in which case it can be re-used).
     *
     * @return true if it can be re-used
     */
    @Override
    public boolean canReuse() {
        return statement.canReuse();
    }

    /**
     * The command is now re-used, therefore reset the canReuse flag, and the
     * parameter values.
     */
    @Override
    public void reuse() {
        statement.reuse();
    }

    @Override
    public boolean isCacheable() {
        return statement.isCacheable();
    }

    @Override
    public int getType() {
        return statement.getType();
    }

    @Override
    public boolean isBatch() {
        return statement.isBatch();
    }

    @Override
    public int hashCode() {
        return statement.hashCode();
    }

    @Override
    public boolean needRecompile() {
        return statement.needRecompile();
    }

    @Override
    public boolean equals(Object obj) {
        return statement.equals(obj);
    }

    @Override
    public void setParameterList(ArrayList<Parameter> parameters) {
        statement.setParameterList(parameters);
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return statement.getParameters();
    }

    @Override
    public boolean isQuery() {
        return statement.isQuery();
    }

    @Override
    public PreparedStatement prepare() {
        statement.prepare();
        return this;
    }

    @Override
    public void setSQL(String sql) {
        statement.setSQL(sql);
    }

    @Override
    public String getSQL() {
        return statement.getSQL();
    }

    @Override
    public String getPlanSQL() {
        return statement.getPlanSQL();
    }

    @Override
    public void setObjectId(int i) {
        statement.setObjectId(i);
    }

    @Override
    public void setSession(ServerSession currentSession) {
        statement.setSession(currentSession);
    }

    @Override
    public void setPrepareAlways(boolean prepareAlways) {
        statement.setPrepareAlways(prepareAlways);
    }

    @Override
    public int getCurrentRowNumber() {
        return statement.getCurrentRowNumber();
    }

    @Override
    public boolean isLocal() {
        return statement.isLocal();
    }

    @Override
    public void setLocal(boolean local) {
        statement.setLocal(local);
    }

    @Override
    public int getFetchSize() {
        return statement.getFetchSize();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        statement.setFetchSize(fetchSize);
    }

    @Override
    public ServerSession getSession() {
        return statement.getSession();
    }

    @Override
    public PreparedStatement getWrappedStatement() {
        return statement;
    }

    @Override
    public Result query(int maxRows) {
        return statement.query(maxRows);
    }

    @Override
    public Result query(int maxRows, boolean scrollable) {
        return statement.query(maxRows, scrollable);
    }

    @Override
    public int update() {
        return statement.update();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object execute(int maxRows, boolean isUpdate, AsyncHandler<AsyncResult<Integer>> updateHandler,
            AsyncHandler<AsyncResult<Result>> queryHandler) {
        boolean async = (updateHandler != null) || (queryHandler != null);
        startTimeNanos = 0;
        long start = 0;
        Database database = session.getDatabase();
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        int savepointId = 0;
        if (isUpdate)
            savepointId = session.getTransaction(statement).getSavepointId();
        else
            session.getTransaction(statement);
        session.setCurrentCommand(this);
        session.addStatement(statement);
        AsyncResult asyncResult = null;
        AsyncHandler asyncHandler = updateHandler != null ? updateHandler : queryHandler;
        try {
            while (true) {
                database.checkPowerOff();
                try {
                    recompileIfRequired();
                    setProgress(DatabaseEventListener.STATE_STATEMENT_START);
                    start();
                    statement.checkParameters();
                    Object result;
                    int rowCount;
                    if (isUpdate) {
                        session.setLastScopeIdentity(ValueNull.INSTANCE);
                        int updateCount = RouterHolder.getRouter().executeUpdate(statement);
                        rowCount = updateCount;
                        if (updateHandler != null) {
                            AsyncResult<Integer> ar = new AsyncResult<>();
                            ar.setResult(updateCount);
                            asyncResult = ar;
                        }
                        result = Integer.valueOf(updateCount);
                    } else {
                        Result r = RouterHolder.getRouter().executeQuery(statement, maxRows);
                        rowCount = r.getRowCount();
                        result = r;
                        if (queryHandler != null) {
                            AsyncResult<Result> ar = new AsyncResult<>();
                            ar.setResult(r);
                            asyncResult = ar;
                        }
                    }
                    statement.trace(startTimeNanos, rowCount);
                    setProgress(DatabaseEventListener.STATE_STATEMENT_END);
                    return result;
                } catch (DbException e) {
                    start = filterConcurrentUpdate(e, start);
                } catch (OutOfMemoryError e) {
                    callStop = false;
                    // there is a serious problem:
                    // the transaction may be applied partially
                    // in this case we need to panic:
                    // close the database
                    database.shutdownImmediately();
                    throw DbException.convert(e);
                } catch (Throwable e) {
                    throw DbException.convert(e);
                }
            }
        } catch (DbException e) {
            e = e.addSQL(statement.getSQL());
            SQLException s = e.getSQLException();
            database.exceptionThrown(s, statement.getSQL());
            if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                callStop = false;
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
                asyncResult = new AsyncResult();
                asyncResult.setCause(e);
                asyncHandler.handle(asyncResult);
                async = false; // 不需要再回调了
                return null;
            } else {
                throw e;
            }
        } finally {
            if (callStop) {
                stop(async, asyncResult, asyncHandler);
            }
        }
    }

    /**
     * Start the stopwatch.
     */
    private void start() {
        if (session.getDatabase().getQueryStatistics() || trace.isInfoEnabled()) {
            startTimeNanos = System.nanoTime();
        }
    }

    private void setProgress(int state) {
        session.getDatabase().setProgress(state, statement.getSQL(), 0, 0);
    }

    private void recompileIfRequired() {
        if (statement.needRecompile()) {
            // TODO test with 'always recompile'
            statement.setModificationMetaId(0);
            String sql = statement.getSQL();
            ArrayList<Parameter> oldParams = statement.getParameters();
            Parser parser = new Parser(session);
            statement = parser.parse(sql);
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

    private long filterConcurrentUpdate(DbException e, long start) {
        if (e.getErrorCode() != ErrorCode.CONCURRENT_UPDATE_1) {
            throw e;
        }
        long now = System.nanoTime() / 1000000;
        if (start != 0 && now - start > session.getLockTimeout()) {
            ArrayList<ServerSession> sessions = session.checkDeadlock();
            if (sessions != null) {
                throw DbException.get(ErrorCode.DEADLOCK_1, StandardTable.getDeadlockDetails(sessions));
            } else {

                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, e.getCause(), "");
            }
        }
        Thread t = Thread.currentThread();
        // 当两个sql执行线程更新同一行出现并发更新冲突时，
        // 不阻塞当前sql执行线程，而是看看是否有其他sql需要执行
        if (t instanceof SQLStatementExecutor) {
            SQLStatementExecutor sqlStatementExecutor = (SQLStatementExecutor) t;
            sqlStatementExecutor.executeNextStatement();
        } else {
            int sleep = 1 + MathUtils.randomInt(10);
            while (true) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e1) {
                    // ignore
                }
                long slept = System.nanoTime() / 1000000 - now;
                if (slept >= sleep) {
                    break;
                }
            }
        }
        return start == 0 ? now : start;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void stop(boolean async, AsyncResult ar, AsyncHandler ah) {
        session.closeTemporaryResults();
        session.setCurrentCommand(null);
        if (async) {
            if (session.isAutoCommit() && session.getReplicationName() == null) { // 在复制模式下不能自动提交
                // 等到事务日志写成功后再返回语句的执行结果
                session.setRunnable(() -> ah.handle(ar));
                session.prepareCommit();
            } else {
                // 当前语句是在一个手动提交的事务中进行，提前返回语句的执行结果
                ah.handle(ar);
            }
        } else {
            if (session.isAutoCommit() && session.getReplicationName() == null) {
                session.commit();
            }
        }
        if (startTimeNanos > 0 && trace.isInfoEnabled()) {
            long timeMillis = (System.nanoTime() - startTimeNanos) / 1000 / 1000;
            // 如果一条sql的执行时间大于100毫秒，记下它
            if (timeMillis > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms, sql: {1}", timeMillis, getSQL());
            }
        }
    }

    @Override
    public Result executeQuery(int maxRows) {
        return (Result) execute(maxRows, false, null, null);
    }

    @Override
    public void executeQueryAsync(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        execute(0, false, null, handler);
    }

    @Override
    public int executeUpdate() {
        return ((Integer) execute(0, true, null, null)).intValue();
    }

    @Override
    public void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        execute(0, true, handler, null);
    }

    @Override
    public int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult) {
        int updateCount = executeUpdate();
        if (commandUpdateResult != null) {
            commandUpdateResult.setUpdateCount(updateCount);
            commandUpdateResult.addResult(this, session.getLastRowKey());
        }
        return updateCount;
    }

    @Override
    public void replicationCommit(long validKey, boolean autoCommit) {
        session.replicationCommit(validKey, autoCommit);
    }

    @Override
    public void replicationRollback() {
        session.rollback();
    }
}
