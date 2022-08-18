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

    protected volatile boolean loopEnd;
    protected int loopCount;
    protected final AtomicInteger updateCount = new AtomicInteger();
    protected final AtomicInteger pendingOperationCount = new AtomicInteger();

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
        if (isCompleted()) {
            setResult(updateCount.get());
        }
    }

    protected abstract void executeLoopUpdate();

    private boolean isCompleted() {
        if (loopEnd && pendingOperationCount.get() <= 0) {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            return true;
        }
        return false;
    }

    protected void onLoopEnd() {
        // 循环已经结束了，但是异步更新可能没有完成，所以先把状态改成STATEMENT_RUNNING，避免调度器空转
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        loopEnd = true;
        // isCompleted(); //在executeInternal()已经调用了
    }

    protected void onComplete(AsyncResult<Integer> ar) {
        if (ar.isSucceeded()) {
            updateCount.incrementAndGet();
        } else {
            setPendingException(ar.getCause());
        }
        // 不能提前，前面设置PendingException后再减，否则主线程调用isCompleted()后就结束了，没有对外抛出异常
        pendingOperationCount.decrementAndGet();

        if (isCompleted()) {
            setResult(updateCount.get());
        }
        // 唤醒调度器，有可能为null，比如启动阶段执行SQL
        if (session.getTransactionListener() != null)
            session.getTransactionListener().wakeUp();
    }
}
