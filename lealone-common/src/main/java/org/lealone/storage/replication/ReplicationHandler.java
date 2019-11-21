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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.lealone.common.concurrent.SimpleCondition;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

abstract class ReplicationHandler<T> implements AsyncHandler<AsyncResult<T>> {

    protected final long start;
    protected final int n;
    protected final AsyncHandler<AsyncResult<T>> topHandler;

    protected final SimpleCondition condition = new SimpleCondition();
    protected final CopyOnWriteArrayList<AsyncResult<T>> results = new CopyOnWriteArrayList<>();

    private final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    @SuppressWarnings("rawtypes")
    private final AtomicIntegerFieldUpdater<ReplicationHandler> failuresUpdater = AtomicIntegerFieldUpdater
            .newUpdater(ReplicationHandler.class, "failures");
    protected volatile int failures = 0;
    protected volatile boolean successful = false;

    public ReplicationHandler(int n, AsyncHandler<AsyncResult<T>> topHandler) {
        start = System.nanoTime();
        this.n = n;
        this.topHandler = topHandler;
    }

    abstract void response(AsyncResult<T> result);

    abstract int totalBlockFor();

    abstract void await(long rpcTimeoutMillis);

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

        if (totalBlockFor() + f >= totalNodes())
            signal();
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
