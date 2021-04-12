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

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

abstract class ReplicationHandler<T> implements AsyncHandler<AsyncResult<T>> {

    private final int totalNodes;
    private final int totalBlockFor;

    protected final AsyncHandler<AsyncResult<T>> finalResultHandler;
    protected final CopyOnWriteArrayList<AsyncResult<T>> results = new CopyOnWriteArrayList<>();

    private final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    // private volatile boolean successful;
    private volatile boolean failed;

    public ReplicationHandler(int totalNodes, int totalBlockFor, AsyncHandler<AsyncResult<T>> finalResultHandler) {
        this.totalNodes = totalNodes;
        this.totalBlockFor = totalBlockFor;
        this.finalResultHandler = finalResultHandler;
    }

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
        // if (!successful && results.size() >= totalBlockFor) {
        // successful = true;
        // onSuccess();
        // }

        if (results.size() >= totalBlockFor) {
            // successful = true;
            onSuccess();
        }
    }

    private synchronized void handleException(Throwable t) {
        exceptions.add(t);
        if (!failed && totalBlockFor + exceptions.size() >= totalNodes) {
            failed = true;
            if (finalResultHandler != null) {
                AsyncResult<T> ar = new AsyncResult<>();
                ar.setCause(exceptions.get(0));
                finalResultHandler.handle(ar);
            }
        }
    }

    synchronized void reset() {
        results.clear();
        exceptions.clear();
        // successful = false;
        failed = false;
    }
}
