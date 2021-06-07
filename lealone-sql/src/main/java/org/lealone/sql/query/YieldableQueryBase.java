/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.sql.StatementBase;
import org.lealone.sql.executor.YieldableBase;

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
