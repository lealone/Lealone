/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.StatementBase;
import org.lealone.sql.router.SQLRouter;

/**
 * This class represents a non-transaction statement, for example a CREATE or DROP.
 */
public abstract class DefinitionStatement extends StatementBase {

    /**
     * Create a new command for the given session.
     *
     * @param session the session
     */
    protected DefinitionStatement(ServerSession session) {
        super(session);
        priority = MIN_PRIORITY;
    }

    @Override
    public Result getMetaData() {
        return null;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean isDDL() {
        return true;
    }

    @Override
    public YieldableDefinitionStatement createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableDefinitionStatement(this, asyncHandler);
    }

    private static class YieldableDefinitionStatement extends YieldableUpdateBase {

        public YieldableDefinitionStatement(DefinitionStatement statement,
                AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            callStop = false;
        }

        @Override
        protected boolean executeInternal() {
            SQLRouter.executeUpdate(statement, ar -> {
                setResult(ar.getResult());
                stop();
            });
            return false;
        }
    }
}
