/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.result.Result;
import com.lealone.db.session.SessionStatus;
import com.lealone.sql.StatementBase;

public class YieldableLocalQuery extends YieldableQueryBase {

    public YieldableLocalQuery(StatementBase statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        super(statement, maxRows, scrollable, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        Result result = statement.query(maxRows);
        setResult(result, result.getRowCount());
        session.setStatus(SessionStatus.STATEMENT_COMPLETED);
    }
}
