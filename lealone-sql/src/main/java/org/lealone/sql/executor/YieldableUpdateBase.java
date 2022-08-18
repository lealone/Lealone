/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.executor;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.sql.StatementBase;

public abstract class YieldableUpdateBase extends YieldableBase<Integer> {

    public YieldableUpdateBase(StatementBase statement,
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    protected void setResult(int updateCount) {
        super.setResult(Integer.valueOf(updateCount), updateCount);
    }
}
