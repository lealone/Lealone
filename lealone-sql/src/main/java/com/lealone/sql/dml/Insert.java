/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.api.Trigger;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.auth.Right;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.sql.SQLStatement;

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
    public YieldableInsert createYieldableUpdate(AsyncResultHandler<Integer> asyncHandler) {
        return new YieldableInsert(this, asyncHandler);
    }

    private static class YieldableInsert extends YieldableMerSert {

        public YieldableInsert(Insert statement, AsyncResultHandler<Integer> asyncHandler) {
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
