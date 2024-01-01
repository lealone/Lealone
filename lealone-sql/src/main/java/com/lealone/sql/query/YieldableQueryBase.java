/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.result.Result;
import com.lealone.sql.StatementBase;
import com.lealone.sql.executor.YieldableBase;

public abstract class YieldableQueryBase extends YieldableBase<Result> {

    protected final int maxRows;
    protected final boolean scrollable;

    public YieldableQueryBase(StatementBase statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        super(statement, asyncHandler);
        this.maxRows = maxRows;
        this.scrollable = scrollable;
    }
}
