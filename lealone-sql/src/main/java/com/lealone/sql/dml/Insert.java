/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
    public YieldableInsert createYieldableUpdate(AsyncResultHandler<Integer> asyncHandler) {
        return new YieldableInsert(this, asyncHandler);
    }

    private static class YieldableInsert extends YieldableMerSert {

        public YieldableInsert(Insert statement, AsyncResultHandler<Integer> asyncHandler) {
            super(statement, asyncHandler);
        }

        @Override
        protected void startInternal() {
            session.getUser().checkRight(table, Right.INSERT);
            table.fire(session, Trigger.INSERT, true);
            super.startInternal();
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
