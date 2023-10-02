/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * INSERT
 * 
 * @author H2 Group
 * @author zhh
 */
public class Insert extends MerSert {

    public Insert(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.INSERT;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(")\n");
        getValuesPlanSQL(buff);
        return buff.toString();
    }

    @Override
    public int update() {
        YieldableInsert yieldable = new YieldableInsert(this, null);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableInsert createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableInsert(this, asyncHandler);
    }

    private static class YieldableInsert extends YieldableMerSert {

        public YieldableInsert(Insert statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
        }

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            session.getUser().checkRight(table, Right.INSERT);
            table.fire(session, Trigger.INSERT, true);
            return super.startInternal();
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.INSERT, false);
        }

        @Override
        protected void merSert(Row row) {
            addRowInternal(row);
        }
    }
}
