/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.executor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement.Yieldable;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.storage.PageKey;

public abstract class YieldableBase<T> implements Yieldable<T> {

    protected StatementBase statement;
    protected final ServerSession session;
    protected final Trace trace;
    protected final AsyncHandler<AsyncResult<T>> asyncHandler;
    protected final boolean async;
    protected AsyncResult<T> asyncResult;
    protected T result;
    protected long startTimeNanos;
    protected boolean started;

    protected volatile Throwable pendingException;
    protected volatile boolean stopped;

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

    protected abstract void executeInternal();

    protected void setResult(T result, int rowCount) {
        this.result = result;
        if (asyncHandler != null && result != null) {
            asyncResult = new AsyncResult<>(result);
        }
        if (rowCount < 0) {
            setProgress(DatabaseEventListener.STATE_STATEMENT_WAITING);
        } else {
            statement.trace(startTimeNanos, rowCount);
            setProgress(DatabaseEventListener.STATE_STATEMENT_END);
        }
    }

    @Override
    public T getResult() {
        return result;
    }

    @Override
    public int getPriority() {
        return statement.getPriority();
    }

    @Override
    public void setPageKeys(List<PageKey> pageKeys) {
        TableFilter tf = statement.getTableFilter();
        if (tf != null)
            tf.setPageKeys(pageKeys);
    }

    @Override
    public final void run() {
        if (!started) {
            if (start()) {
                session.setStatus(SessionStatus.STATEMENT_RUNNING);
                return;
            }
            started = true;
        }

        execute();

        if (pendingException != null) {
            handleException(DbException.convert(pendingException));
        } else if (session.getStatus() == SessionStatus.STATEMENT_COMPLETED) {
            stop();
        }
    }

    private boolean start() {
        if (session.isExclusiveMode())
            return true;
        if (session.getDatabase().getQueryStatistics() || trace.isInfoEnabled()) {
            startTimeNanos = System.nanoTime();
        }
        recompileIfNeeded();
        session.startCurrentCommand(statement);
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

    private void execute() {
        try {
            session.getDatabase().checkPowerOff();
            executeInternal();
        } catch (DbException e) {
            // 并发异常，直接重试
            if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1) {
                return;
            }
            handleException(e);
        } catch (Throwable e) {
            handleException(DbException.convert(e));
        }
    }

    protected void setPendingException(Throwable pendingException) {
        if (this.pendingException == null)
            this.pendingException = pendingException;
    }

    private void handleException(DbException e) {
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
        if (!statement.isQuery()) {
            if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                session.rollback();
            } else {
                session.rollbackCurrentCommand();
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

    @Override
    public void stop() {
        stopped = true;
        stopInternal();
        session.stopCurrentCommand(asyncHandler, asyncResult);

        if (startTimeNanos > 0 && trace.isInfoEnabled()) {
            long timeMillis = (System.nanoTime() - startTimeNanos) / 1000 / 1000;
            // 如果一条sql的执行时间大于100毫秒，记下它
            if (timeMillis > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms, sql: {1}", timeMillis, statement.getSQL());
            }
        }
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void back() {
        // do nothing
    }

    public boolean yieldIfNeeded(int rowNumber) {
        return async && statement.setCurrentRowNumber(rowNumber);
    }
}
