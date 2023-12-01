/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

public abstract class SchedulerListener<R> implements AsyncHandler<AsyncResult<R>> {

    protected volatile R result;
    protected volatile DbException exception;
    protected boolean needWakeUp = true;

    public void setNeedWakeUp(boolean needWakeUp) {
        this.needWakeUp = needWakeUp;
    }

    public void setResult(R r) {
        handle(new AsyncResult<>(r));
    }

    public void setException(Throwable t) {
        handle(new AsyncResult<>(t));
    }

    @Override
    public void handle(AsyncResult<R> ar) {
        if (ar.isSucceeded()) {
            result = ar.getResult();
        } else {
            exception = DbException.convert(ar.getCause());
        }
        if (needWakeUp)
            wakeUp();
    }

    public abstract R await();

    public abstract void wakeUp();

    public static interface Factory {
        <R> SchedulerListener<R> createSchedulerListener();
    }

    public static <R> SchedulerListener<R> createSchedulerListener() {
        Object object = SchedulerThread.currentObject();
        if (object instanceof SchedulerListener.Factory) {
            return ((SchedulerListener.Factory) object).createSchedulerListener();
        } else {
            // 创建一个同步阻塞监听器
            return new SchedulerListener<R>() {

                private final CountDownLatch latch = new CountDownLatch(1);

                @Override
                public R await() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        exception = DbException.convert(e);
                    }
                    if (exception != null)
                        throw exception;
                    return result;
                }

                @Override
                public void wakeUp() {
                    latch.countDown();
                }
            };
        }
    }
}
