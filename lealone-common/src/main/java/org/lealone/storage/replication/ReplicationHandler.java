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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

abstract class ReplicationHandler<T> implements AsyncHandler<AsyncResult<T>> {

    protected final AsyncHandler<AsyncResult<T>> finalResultHandler;
    protected final ArrayList<AsyncResult<T>> results = new ArrayList<>();
    private final ArrayList<Throwable> exceptions = new ArrayList<>();
    private final int ackNodes; // 需要收到响应的节点数
    private int ackCount; // 已收到响应数

    public ReplicationHandler(int ackNodes, AsyncHandler<AsyncResult<T>> finalResultHandler) {
        this.ackNodes = ackNodes;
        this.finalResultHandler = finalResultHandler;
    }

    abstract void onSuccess();

    @Override
    public synchronized void handle(AsyncResult<T> ar) {
        ackCount++;
        if (ar.isSucceeded()) {
            handleResult(ar);
        } else {
            handleException(ar.getCause());
        }
    }

    private void handleResult(AsyncResult<T> result) {
        results.add(result);
        if (ackCount >= ackNodes) {
            onSuccess();
        }
    }

    private void handleException(Throwable t) {
        int errorCode = DbException.convert(t).getErrorCode();
        if (errorCode == ErrorCode.CONNECTION_BROKEN_1 || errorCode == ErrorCode.NETWORK_TIMEOUT_1) {
            // TODO
        }
        exceptions.add(t);
        if (ackCount >= ackNodes && finalResultHandler != null) {
            AsyncResult<T> ar = new AsyncResult<>();
            ar.setCause(exceptions.get(0));
            finalResultHandler.handle(ar);
        }
    }
}
