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
package org.lealone.storage.replication;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.lealone.common.concurrent.SimpleCondition;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;
import org.lealone.storage.replication.exceptions.WriteFailureException;
import org.lealone.storage.replication.exceptions.WriteTimeoutException;

abstract class ReplicationHandler<T> implements AsyncHandler<AsyncResult<T>> {

    protected final long start;
    protected final int n;
    protected final AsyncHandler<AsyncResult<T>> topHandler;
    protected final CopyOnWriteArrayList<AsyncResult<T>> results = new CopyOnWriteArrayList<>();

    private final SimpleCondition condition = new SimpleCondition();
    private final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    @SuppressWarnings("rawtypes")
    private final AtomicIntegerFieldUpdater<ReplicationHandler> failuresUpdater = AtomicIntegerFieldUpdater
            .newUpdater(ReplicationHandler.class, "failures");
    private volatile int failures = 0;
    protected volatile boolean successful = false;

    public ReplicationHandler(int n, AsyncHandler<AsyncResult<T>> topHandler) {
        start = System.nanoTime();
        this.n = n;
        this.topHandler = topHandler;
    }

    abstract void response(AsyncResult<T> result);

    abstract int totalBlockFor();

    abstract boolean isRead();

    void await(long rpcTimeoutMillis) {
        // 超时时间把调用构造函数开始直到调用get前的这段时间也算在内
        long timeout = rpcTimeoutMillis > 0
                ? TimeUnit.MILLISECONDS.toNanos(rpcTimeoutMillis) - (System.nanoTime() - start)
                : rpcTimeoutMillis;

        boolean success;
        try {
            if (timeout > 0) {
                success = condition.await(timeout, TimeUnit.NANOSECONDS);
            } else {
                condition.await();
                success = true;
            }
        } catch (InterruptedException ex) {
            throw new AssertionError(ex);
        }
        if (!successful) {
            if (!success) {
                int blockedFor = totalBlockFor();
                int acks = ackCount();
                // It's pretty unlikely, but we can race between exiting await above and here, so
                // that we could now have enough acks. In that case, we "lie" on the acks count to
                // avoid sending confusing info to the user (see CASSANDRA-6491).
                if (acks >= blockedFor)
                    acks = blockedFor - 1;
                if (isRead())
                    throw new ReadTimeoutException(ConsistencyLevel.QUORUM, acks, blockedFor, false);
                else
                    throw new WriteTimeoutException(ConsistencyLevel.QUORUM, acks, blockedFor);
            }

            if (totalBlockFor() + failures >= totalNodes()) {
                if (isRead())
                    throw new ReadFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor(),
                            false);
                else
                    throw new WriteFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor());
            }
        }
    }

    T getResult(long rpcTimeoutMillis) {
        await(rpcTimeoutMillis);
        return results.get(0).getResult();
    }

    void initCause(Throwable e) {
        if (!exceptions.isEmpty())
            e.initCause(exceptions.get(0));
    }

    private void onFailure() {
        int f = failuresUpdater.incrementAndGet(this);

        if (totalBlockFor() + f >= totalNodes()) {
            signal();
            if (topHandler != null) {
                AsyncResult<T> ar = new AsyncResult<>();
                ar.setCause(exceptions.get(0));
                topHandler.handle(ar);
            }
        }
    }

    void signal() {
        condition.signalAll();
    }

    int totalNodes() {
        return n;
    }

    int ackCount() {
        return results.size();
    }

    @Override
    public void handle(AsyncResult<T> ar) {
        if (ar.isSucceeded()) {
            response(ar);
        } else {
            exceptions.add(ar.getCause());
            onFailure();
        }
    }
}
