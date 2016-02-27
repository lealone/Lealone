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
package org.lealone.replication;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.lealone.common.concurrent.SimpleCondition;
import org.lealone.common.util.New;
import org.lealone.replication.exceptions.WriteFailureException;
import org.lealone.replication.exceptions.WriteTimeoutException;

class WriteResponseHandler {
    private final SimpleCondition condition = new SimpleCondition();
    private final long start;

    private final ArrayList<Integer> updateCountList;
    private final ArrayList<Object> resultList;
    private final int n;
    private final int w;

    private final AtomicIntegerFieldUpdater<WriteResponseHandler> failuresUpdater = AtomicIntegerFieldUpdater
            .newUpdater(WriteResponseHandler.class, "failures");
    private volatile int failures = 0;

    private volatile boolean successful = false;

    WriteResponseHandler(int n) {
        start = System.nanoTime();

        this.n = n;
        // w = n / 2 + 1;
        w = n; // 使用Write all read one模式
        updateCountList = New.arrayList(n);
        resultList = New.arrayList(n);
    }

    synchronized void response(int updateCount) {
        updateCountList.add(updateCount);
        if (!successful && updateCountList.size() >= w) {
            successful = true;
            signal();
        }
    }

    synchronized void response(Object result) {
        resultList.add(result);
        if (!successful && resultList.size() >= w) {
            successful = true;
            signal();
        }
    }

    void onFailure() {
        int f = failuresUpdater.incrementAndGet(this);

        if (totalBlockFor() + f >= totalEndpoints())
            signal();
    }

    int get(long rpcTimeoutMillis) {
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
            throw new WriteTimeoutException(ConsistencyLevel.QUORUM, acks, blockedFor);
        }

        if (!successful && totalBlockFor() + failures >= totalEndpoints()) {
            throw new WriteFailureException(ConsistencyLevel.QUORUM, ackCount(), failures, totalBlockFor());
        }

        return updateCountList.get(0);
    }

    private void signal() {
        condition.signalAll();
    }

    private int totalBlockFor() {
        return w;
    }

    private int totalEndpoints() {
        return n;
    }

    private int ackCount() {
        return updateCountList.size();
    }

    int getFailures() {
        return failures;
    }

    boolean isSuccessful() {
        return successful;
    }
}
