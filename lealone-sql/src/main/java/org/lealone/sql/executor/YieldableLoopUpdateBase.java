/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.executor;

import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.StatementBase;

public abstract class YieldableLoopUpdateBase extends YieldableUpdateBase {

    protected final AtomicInteger updateCount = new AtomicInteger();
    protected int loopCount;
    private boolean loopEnd;
    private int pendingOperationCount;

    public YieldableLoopUpdateBase(StatementBase statement,
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        if (!loopEnd) {
            executeLoopUpdate();
            if (session.getStatus() == SessionStatus.WAITING) {
                return;
            }
        }
        handleResult();
    }

    protected abstract void executeLoopUpdate();

    private void handleResult() {
        if (loopEnd && pendingOperationCount <= 0) {
            setResult(updateCount.get());
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }

    protected void onLoopEnd() {
        // 循环已经结束了，但是异步更新可能没有完成，所以先把状态改成STATEMENT_RUNNING，避免调度器空转
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        loopEnd = true;
    }

    protected void onPendingOperationStart() {
        pendingOperationCount++;
    }

    // 执行回调的线程跟执行命令的线程都是同一个
    protected void onPendingOperationComplete(AsyncResult<Integer> ar) {
        if (ar.isSucceeded()) {
            updateCount.incrementAndGet();
        } else {
            setPendingException(ar.getCause());
        }
        pendingOperationCount--;
        handleResult();
    }
}
