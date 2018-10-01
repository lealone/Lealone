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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.lealone.common.concurrent.SimpleCondition;
import org.lealone.db.result.Result;
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;

class ReadResponseHandler {
    private final SimpleCondition condition = new SimpleCondition();
    private final long start;

    private final ArrayList<Result> results;
    private final ArrayList<Object> resultObjects;
    private final int n;
    private final int r;

    private final AtomicIntegerFieldUpdater<ReadResponseHandler> failuresUpdater = AtomicIntegerFieldUpdater
            .newUpdater(ReadResponseHandler.class, "failures");
    private volatile int failures = 0;

    private volatile boolean successful = false;

    ReadResponseHandler(int n) {
        start = System.nanoTime();

        this.n = n;
        // r = n / 2 + 1;
        r = 1; // 使用Write all read one模式
        results = new ArrayList<>(n);
        resultObjects = new ArrayList<>(n);
    }

    synchronized void response(Result result) {
        results.add(result);

        if (!successful && results.size() >= r) {
            successful = true;
            signal();
        }
    }

    synchronized void response(Object result) {
        resultObjects.add(result);

        if (!successful && resultObjects.size() >= r) {
            successful = true;
            signal();
        }
    }

    void onFailure() {
        int f = failuresUpdater.incrementAndGet(this);

        if (totalBlockFor() + f >= totalEndpoints())
            signal();
    }

    Result get(long rpcTimeoutMillis) {
        long requestTimeout = rpcTimeoutMillis;

        // 超时时间把调用构造函数开始直到调用get前的这段时间也算在内
        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);

        boolean success;
        try {
            success = condition.await(timeout, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            throw new AssertionError(ex);
        }

        if (!success) {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new ReadTimeoutException(ConsistencyLevel.QUORUM, acks, blockedFor, false);
        }

        if (!successful && totalBlockFor() + failures >= totalEndpoints()) {
            throw new ReadFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor(), false);
        }

        return results.get(0);
    }

    Object getResultObject(long rpcTimeoutMillis) {
        long requestTimeout = rpcTimeoutMillis;

        // 超时时间把调用构造函数开始直到调用get前的这段时间也算在内
        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);

        boolean success;
        try {
            success = condition.await(timeout, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            throw new AssertionError(ex);
        }

        if (!success) {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new ReadTimeoutException(ConsistencyLevel.QUORUM, acks, blockedFor, false);
        }

        if (!successful && totalBlockFor() + failures >= totalEndpoints()) {
            throw new ReadFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor(), false);
        }

        return resultObjects.get(0);
    }

    void signal() {
        condition.signalAll();
    }

    int totalBlockFor() {
        return r;
    }

    int totalEndpoints() {
        return n;
    }

    int ackCount() {
        return results.size();
    }

    int getFailures() {
        return failures;
    }

    boolean isSuccessful() {
        return successful;
    }

}
