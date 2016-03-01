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
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.MathUtils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.router.RouterHolder;

/**
 * Represents a SQL statement wrapper.
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
    private long startTime;

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
     * Start the stopwatch.
     */
    void start() {
        if (trace.isInfoEnabled()) {
            startTime = System.currentTimeMillis();
        }
    }

    void setProgress(int state) {
        session.getDatabase().setProgress(state, statement.getSQL(), 0, 0);
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

    private void stop() {
        session.closeTemporaryResults();
        session.setCurrentCommand(null);
        if (!isTransactional()) {
            session.commit(true);
        } else if (session.isAutoCommit()) {
            session.commit(false);
        } else if (session.getDatabase().isMultiThreaded()) {
            Database db = session.getDatabase();
            if (db != null) {
                if (db.getLockMode() == Constants.LOCK_MODE_READ_COMMITTED) {
                    session.unlockReadLocks();
                }
            }
        }
        if (trace.isInfoEnabled() && startTime > 0) {
            long time = System.currentTimeMillis() - startTime;
            if (time > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms", time);
            }
        }
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
        startTime = 0;
        long start = 0;
        Database database = session.getDatabase();
        Object sync = database.isMultiThreaded() ? session : database;
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        synchronized (sync) {
            session.setCurrentCommand(this);
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        return queryInternal(maxRows);
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
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
            }
        }
    }

    private Result queryInternal(int maxRows) {
        recompileIfRequired();
        setProgress(DatabaseEventListener.STATE_STATEMENT_START);
        start();
        statement.checkParameters();
        Result result = RouterHolder.getRouter().executeQuery(statement, maxRows);
        statement.trace(startTime, result.getRowCount());
        setProgress(DatabaseEventListener.STATE_STATEMENT_END);
        return result;
    }

    @Override
    public int update() {
        long start = 0;
        Database database = session.getDatabase();
        Object sync = database.isMultiThreaded() ? session : database;
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        synchronized (sync) {
            int savepointId = session.getTransaction(statement).getSavepointId();
            session.setCurrentCommand(this);
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        return updateInternal();
                    } catch (DbException e) {
                        start = filterConcurrentUpdate(e, start);
                    } catch (OutOfMemoryError e) {
                        callStop = false;
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
                if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                    session.rollback();
                } else {
                    session.rollbackTo(savepointId);
                }
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
            }
        }
    }

    private int updateInternal() {
        recompileIfRequired();
        setProgress(DatabaseEventListener.STATE_STATEMENT_START);
        start();
        session.setLastScopeIdentity(ValueNull.INSTANCE);
        statement.checkParameters();
        int updateCount = RouterHolder.getRouter().executeUpdate(statement);
        statement.trace(startTime, updateCount);
        setProgress(DatabaseEventListener.STATE_STATEMENT_END);
        return updateCount;
    }

    private long filterConcurrentUpdate(DbException e, long start) {
        if (e.getErrorCode() != ErrorCode.CONCURRENT_UPDATE_1) {
            throw e;
        }
        long now = System.nanoTime() / 1000000;
        if (start != 0 && now - start > session.getLockTimeout()) {
            throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, e.getCause(), "");
        }
        Database database = session.getDatabase();
        int sleep = 1 + MathUtils.randomInt(10);
        while (true) {
            try {
                if (database.isMultiThreaded()) {
                    Thread.sleep(sleep);
                } else {
                    database.wait(sleep);
                }
            } catch (InterruptedException e1) {
                // ignore
            }
            long slept = System.nanoTime() / 1000000 - now;
            if (slept >= sleep) {
                break;
            }
        }
        return start == 0 ? now : start;
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

    private void recompileIfRequired() {
        if (statement.needRecompile()) {
            // TODO test with 'always recompile'
            statement.setModificationMetaId(0);
            String sql = statement.getSQL();
            ArrayList<Parameter> oldParams = statement.getParameters();
            Parser parser = new Parser(session);
            statement = (StatementBase) parser.parse(sql).prepare();
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

    private boolean readOnlyKnown;
    private boolean readOnly;

    @Override
    public boolean isReadOnly() {
        if (!readOnlyKnown) {
            readOnly = statement.isReadOnly();
            readOnlyKnown = true;
        }
        return readOnly;
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
    public boolean isTransactional() {
        return statement.isTransactional();
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
}
