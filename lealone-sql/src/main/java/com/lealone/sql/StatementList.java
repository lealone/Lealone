/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql;

import java.util.ArrayList;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.SessionStatus;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.expression.Parameter;

/**
 * Represents a list of SQL statements.
 * 
 * @author H2 Group
 * @author zhh
 */
public class StatementList extends StatementBase {

    private final StatementBase firstStatement;
    private final String remaining;

    public StatementList(ServerSession session, StatementBase firstStatement, String remaining) {
        super(session);
        this.firstStatement = firstStatement;
        this.remaining = remaining;
    }

    public StatementBase getFirstStatement() {
        return firstStatement;
    }

    public String getRemaining() {
        return remaining;
    }

    @Override
    public int getType() {
        return firstStatement.getType();
    }

    @Override
    public Future<Result> getMetaData() {
        return firstStatement.getMetaData();
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return firstStatement.getParameters();
    }

    @Override
    public PreparedSQLStatement prepare() {
        firstStatement.prepare();
        return this;
    }

    @Override
    public boolean isQuery() {
        return firstStatement.isQuery();
    }

    @Override
    public Result query(int maxRows) {
        Result result = firstStatement.query(maxRows);
        executeRemaining();
        return result;
    }

    @Override
    public int update() {
        int updateCount = firstStatement.update();
        executeRemaining();
        return updateCount;
    }

    private void executeRemaining() {
        StatementBase remainingStatement = (StatementBase) session.prepareStatement(remaining, -1);
        if (remainingStatement.isQuery()) {
            remainingStatement.query(0);
        } else {
            remainingStatement.update();
        }
    }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        return new YieldableStatementList<>(this, maxRows, scrollable, asyncHandler, true);
    }

    @Override
    public YieldableBase<Integer> createYieldableUpdate(
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableStatementList<>(this, -1, false, asyncHandler, false);
    }

    private static class YieldableStatementList<T> extends YieldableBase<T> {

        private ServerSession nestedSession;
        private String[] statements;
        private Yieldable<?> yieldable;
        private Result result;
        private int updateCount;
        private int index;
        private boolean executeQuery;

        private YieldableStatementList(StatementList statementList, int maxRows, boolean scrollable,
                AsyncHandler<AsyncResult<T>> asyncHandler, boolean executeQuery) {
            super(statementList, asyncHandler);
            nestedSession = session.createNestedSession();
            if (session.isAutoCommit())
                nestedSession.setAutoCommit(false);
            StatementBase firstStatement = statementList.firstStatement;
            firstStatement.setSession(nestedSession); // 要切换session
            statements = statementList.getRemaining().split(";");
            if (firstStatement.isQuery())
                createYieldableQuery(firstStatement, maxRows, scrollable);
            else
                createYieldableUpdate(firstStatement);
            this.executeQuery = executeQuery;
        }

        private void createYieldableUpdate(PreparedSQLStatement stmt) {
            yieldable = stmt.createYieldableUpdate(ar -> {
                if (ar.isSucceeded()) {
                    updateCount += ar.getResult();
                    if (index >= statements.length) {
                        onComplete(null);
                    } else {
                        nestedSession.getSessionInfo().submitTask(() -> {
                            PreparedSQLStatement s = nestedSession.prepareStatement(statements[index++]);
                            if (s.isQuery())
                                createYieldableQuery(s, -1, false);
                            else
                                createYieldableUpdate(s);
                        });
                    }
                } else {
                    onComplete(ar.getCause());
                }
            });
            YieldableCommand c = new YieldableCommand(-1, yieldable, -1);
            nestedSession.setYieldableCommand(c);
        }

        private void createYieldableQuery(PreparedSQLStatement stmt, int maxRows, boolean scrollable) {
            yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
                if (ar.isSucceeded()) {
                    if (result == null)
                        result = ar.getResult();
                    if (index >= statements.length) {
                        onComplete(null);
                    } else {
                        nestedSession.getSessionInfo().submitTask(() -> {
                            PreparedSQLStatement s = nestedSession.prepareStatement(statements[index++]);
                            if (s.isQuery())
                                createYieldableQuery(s, maxRows, scrollable);
                            else
                                createYieldableUpdate(s);
                        });
                    }
                } else {
                    onComplete(ar.getCause());
                }
            });
            YieldableCommand c = new YieldableCommand(-1, yieldable, -1);
            nestedSession.setYieldableCommand(c);
        }

        private void onComplete(Throwable t) {
            yieldable = null;
            statements = null;
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            if (t != null)
                setPendingException(t);
            if (session.isAutoCommit()) {
                if (t != null)
                    nestedSession.rollback();
                else
                    nestedSession.asyncCommit();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void executeInternal() {
            if (yieldable == null) {
                if (executeQuery)
                    setResult((T) result, result.getRowCount());
                else
                    setResult((T) Integer.valueOf(updateCount), updateCount);
            } else {
                session.setStatus(SessionStatus.STATEMENT_RUNNING);
            }
        }
    }
}
