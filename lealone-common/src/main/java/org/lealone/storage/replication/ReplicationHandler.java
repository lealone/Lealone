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

    private final long start;
    private final int totalNodes;
    private final int totalBlockFor;

    protected final AsyncHandler<AsyncResult<T>> finalResultHandler;
    protected final CopyOnWriteArrayList<AsyncResult<T>> results = new CopyOnWriteArrayList<>();

    private final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
    private final SimpleCondition condition = new SimpleCondition();

    @SuppressWarnings("rawtypes")
    private final AtomicIntegerFieldUpdater<ReplicationHandler> failuresUpdater = AtomicIntegerFieldUpdater
            .newUpdater(ReplicationHandler.class, "failures");
    private volatile int failures;
    private volatile boolean successful;

    public ReplicationHandler(int totalNodes, int totalBlockFor, AsyncHandler<AsyncResult<T>> finalResultHandler) {
        start = System.nanoTime();
        this.totalNodes = totalNodes;
        this.totalBlockFor = totalBlockFor;
        this.finalResultHandler = finalResultHandler;
    }

    abstract boolean isRead();

    abstract void onSuccess();

    @Override
    public void handle(AsyncResult<T> ar) {
        if (ar.isSucceeded()) {
            handleResult(ar);
        } else {
            handleException(ar.getCause());
        }
    }

    private synchronized void handleResult(AsyncResult<T> result) {
        results.add(result);
        if (!successful && results.size() >= totalBlockFor) {
            successful = true;
            try {
                onSuccess();
            } finally {
                signal();
            }
        }
    }

    private synchronized void handleException(Throwable t) {
        exceptions.add(t);
        int f = failuresUpdater.incrementAndGet(this);
        if (totalBlockFor + f >= totalNodes) {
            try {
                if (finalResultHandler != null) {
                    AsyncResult<T> ar = new AsyncResult<>();
                    ar.setCause(exceptions.get(0));
                    finalResultHandler.handle(ar);
                }
            } finally {
                signal();
            }
        }
    }

    private void signal() {
        condition.signalAll();
    }

    @Deprecated
    T getResult(long rpcTimeoutMillis) {
        await(rpcTimeoutMillis);
        return results.get(0).getResult();
    }

    @Deprecated
    void initCause(Throwable e) {
        if (!exceptions.isEmpty())
            e.initCause(exceptions.get(0));
    }

    private int ackCount() {
        return results.size();
    }

    private void await(long rpcTimeoutMillis) {
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
                int blockedFor = totalBlockFor;
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

            if (totalBlockFor + failures >= totalNodes) {
                if (isRead())
                    throw new ReadFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor, false);
                else
                    throw new WriteFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor);
            }
        }
    }
}
