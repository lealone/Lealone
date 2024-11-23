/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.executor;

import com.lealone.db.async.AsyncResultHandler;
import com.lealone.sql.StatementBase;

public abstract class YieldableUpdateBase extends YieldableBase<Integer> {

    public YieldableUpdateBase(StatementBase statement, AsyncResultHandler<Integer> asyncHandler) {
        super(statement, asyncHandler);
    }

    protected void setResult(int updateCount) {
        super.setResult(Integer.valueOf(updateCount), updateCount);
    }
}
