/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.Packet;

@SuppressWarnings("rawtypes")
public class AsyncCallback<T> implements Future<T> {

    private static final AtomicReferenceFieldUpdater<AsyncCallback, AsyncHandler> cUpdater = AtomicReferenceFieldUpdater
            .newUpdater(AsyncCallback.class, AsyncHandler.class, "completeHandler");
    private static final AtomicReferenceFieldUpdater<AsyncCallback, AsyncHandler> sUpdater = AtomicReferenceFieldUpdater
            .newUpdater(AsyncCallback.class, AsyncHandler.class, "successHandler");
    private static final AtomicReferenceFieldUpdater<AsyncCallback, AsyncHandler> fUpdater = AtomicReferenceFieldUpdater
            .newUpdater(AsyncCallback.class, AsyncHandler.class, "failureHandler");

    protected volatile boolean runEnd;
    protected volatile AsyncHandler<AsyncResult<T>> completeHandler;
    protected volatile AsyncHandler<T> successHandler;
    protected volatile AsyncHandler<Throwable> failureHandler;
    protected volatile AsyncResult<T> asyncResult;

    protected final AtomicReference<LatchObject> latchObjectRef = new AtomicReference<>();

    private static class LatchObject {
        final CountDownLatch latch;

        public LatchObject(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    public AsyncCallback() {
    }

    public void setDbException(DbException e, boolean cancel) {
        setAsyncResult(e);
        if (cancel)
            countDown();
    }

    private T await(long timeoutMillis) {
        if (latchObjectRef.compareAndSet(null, new LatchObject(new CountDownLatch(1)))) {
            CountDownLatch latch = latchObjectRef.get().latch;
            try {
                if (timeoutMillis > 0)
                    latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
                else
                    latch.await();
                if (asyncResult != null && asyncResult.isFailed())
                    throw DbException.convert(asyncResult.getCause());

                // 如果没有执行过run，抛出合适的异常
                if (!runEnd) {
                    handleTimeout();
                }
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            }
            if (asyncResult != null)
                return asyncResult.getResult();
            else
                return null;
        } else {
            if (asyncResult.isFailed())
                throw DbException.convert(asyncResult.getCause());
            else
                return asyncResult.getResult();
        }
    }

    public final void run(NetInputStream in) {
        // 放在最前面，不能放在最后面，
        // 否则调用了countDown，但是在设置runEnd为true前，调用await的线程读到的是false就会抛异常
        runEnd = true;
        // if (asyncResult == null) {
        try {
            runInternal(in);
        } catch (Throwable t) {
            setAsyncResult(t);
        }
        // }
    }

    protected void runInternal(NetInputStream in) throws Exception {
    }

    @Override
    public T get() {
        long timeoutMillis = networkTimeout > 0 ? networkTimeout : -1;
        return await(timeoutMillis);
    }

    @Override
    public T get(long timeoutMillis) {
        return await(timeoutMillis);
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        successHandler = handler;
        if (asyncResult != null && asyncResult.isSucceeded()) {
            if (sUpdater.compareAndSet(this, successHandler, null)) {
                handler.handle(asyncResult.getResult());
            }
        }
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        failureHandler = handler;
        if (asyncResult != null && asyncResult.isFailed()) {
            if (fUpdater.compareAndSet(this, failureHandler, null)) {
                handler.handle(asyncResult.getCause());
            }
        }
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        completeHandler = handler;
        if (asyncResult != null) {
            if (cUpdater.compareAndSet(this, completeHandler, null)) {
                handler.handle(asyncResult);
            }
        }
        return this;
    }

    public void setAsyncResult(Throwable cause) {
        setAsyncResult(new AsyncResult<>(cause));
    }

    public void setAsyncResult(T result) {
        setAsyncResult(new AsyncResult<>(result));
    }

    public void setAsyncResult(AsyncResult<T> asyncResult) {
        runEnd = true;
        this.asyncResult = asyncResult;
        try {
            AsyncHandler<AsyncResult<T>> cHandler = completeHandler;
            if (cHandler != null && cUpdater.compareAndSet(this, completeHandler, null)) {
                cHandler.handle(asyncResult);
            }

            if (asyncResult != null && asyncResult.isSucceeded()) {
                AsyncHandler<T> handler = successHandler;
                if (handler != null && sUpdater.compareAndSet(this, successHandler, null)) {
                    handler.handle(asyncResult.getResult());
                }
            }

            if (asyncResult != null && asyncResult.isFailed()) {
                AsyncHandler<Throwable> handler = failureHandler;
                if (handler != null && fUpdater.compareAndSet(this, failureHandler, null)) {
                    handler.handle(asyncResult.getCause());
                }
            }
        } finally {
            countDown();
        }
    }

    private void countDown() {
        if (!latchObjectRef.compareAndSet(null, new LatchObject(null))) {
            CountDownLatch latch = latchObjectRef.get().latch;
            // 被调用多次时可能为null
            if (latch != null)
                latch.countDown();
        }
    }

    private Packet packet;
    private long startTime;
    private int networkTimeout;

    public void setPacket(Packet packet) {
        this.packet = packet;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getNetworkTimeout() {
        return networkTimeout;
    }

    public void setNetworkTimeout(int networkTimeout) {
        this.networkTimeout = networkTimeout;
    }

    public void checkTimeout(long currentTime) {
        if (networkTimeout <= 0 || startTime <= 0 || startTime + networkTimeout > currentTime)
            return;
        handleTimeout();
    }

    private void handleTimeout() {
        String msg = "ack timeout, request start time: " + new java.sql.Timestamp(startTime) //
                + ", network timeout: " + networkTimeout + "ms" //
                + ", request packet: " + packet;
        DbException e = DbException.get(ErrorCode.NETWORK_TIMEOUT_1, msg);
        setAsyncResult(e);
        networkTimeout = 0;
    }
}
