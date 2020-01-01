/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.db.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.NetInputStream;

public class AsyncCallback<T> implements Future<T> {

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
                    throw new RuntimeException("time out");
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
        if (asyncResult == null) {
            try {
                runInternal(in);
            } catch (Throwable t) {
                setAsyncResult(t);
            }
        }
    }

    protected void runInternal(NetInputStream in) throws Exception {
    }

    @Override
    public T get() {
        return await(-1);
    }

    @Override
    public T get(long timeoutMillis) {
        return await(timeoutMillis);
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        successHandler = handler;
        if (asyncResult != null && asyncResult.isSucceeded())
            handler.handle(asyncResult.getResult());
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        failureHandler = handler;
        if (asyncResult != null && asyncResult.isFailed())
            handler.handle(asyncResult.getCause());
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        completeHandler = handler;
        if (asyncResult != null)
            handler.handle(asyncResult);
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
            if (completeHandler != null)
                completeHandler.handle(asyncResult);

            if (successHandler != null && asyncResult != null && asyncResult.isSucceeded())
                successHandler.handle(asyncResult.getResult());

            if (failureHandler != null && asyncResult != null && asyncResult.isFailed())
                failureHandler.handle(asyncResult.getCause());
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
}
