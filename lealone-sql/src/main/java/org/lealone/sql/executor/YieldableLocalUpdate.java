/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.executor;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.StatementBase;

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

        // 返回的值为负数时，表示当前语句无法正常执行，需要等待其他事务释放锁
        if (updateCount < 0) {
            session.setStatus(SessionStatus.WAITING);
        } else {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }
}
