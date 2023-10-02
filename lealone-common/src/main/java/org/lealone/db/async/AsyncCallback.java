/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.Packet;

public abstract class AsyncCallback<T> implements Future<T> {

    public AsyncCallback() {
    }

    public void setDbException(DbException e, boolean cancel) {
    }

    public void run(NetInputStream in) {
    }

    protected void runInternal(NetInputStream in) throws Exception {
    }

    protected abstract T await(long timeoutMillis);

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
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        return this;
    }

    public void setAsyncResult(Throwable cause) {
        setAsyncResult(new AsyncResult<>(cause));
    }

    public void setAsyncResult(T result) {
        setAsyncResult(new AsyncResult<>(result));
    }

    public void setAsyncResult(AsyncResult<T> asyncResult) {
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

    protected void handleTimeout() {
        String msg = "ack timeout, request start time: " + new java.sql.Timestamp(startTime) //
                + ", network timeout: " + networkTimeout + "ms" //
                + ", request packet: " + packet;
        DbException e = DbException.get(ErrorCode.NETWORK_TIMEOUT_1, msg);
        setAsyncResult(e);
        networkTimeout = 0;
    }

    public static <T> AsyncCallback<T> createSingleThreadCallback() {
        return new SingleThreadAsyncCallback<>();
    }

    public static <T> AsyncCallback<T> createConcurrentCallback() {
        return new ConcurrentAsyncCallback<>();
    }

    public static <T> AsyncCallback<T> create(boolean isSingleThread) {
        return isSingleThread ? createSingleThreadCallback() : createConcurrentCallback();
    }
}
