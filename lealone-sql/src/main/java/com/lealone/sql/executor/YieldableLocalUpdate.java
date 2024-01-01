/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.executor;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.session.SessionStatus;
import com.lealone.sql.StatementBase;

public class YieldableLocalUpdate extends YieldableUpdateBase {

    public YieldableLocalUpdate(StatementBase statement,
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        int updateCount = statement.update();
        setResult(updateCount);

        // 返回的值为负数时，表示当前语句无法正常执行，需要等待其他事务释放锁。
        // 当 updateCount<0 时不能再设置为 WAITING 状态，
        // 一方面已经设置过了，另一方面如果此时其他事务释放锁了再设置会导致当前语句在后续无法执行。
        if (updateCount >= 0) {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }
}
