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
import java.util.List;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

class WriteResponseHandler<T> extends ReplicationHandler<T> {

    static interface ReplicationResultHandler<T> {
        T handleResults(List<T> results);
    }

    private final int w;
    private final ReplicaCommand[] commands;
    private final ReplicationResultHandler<T> replicationResultHandler;

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands,
            AsyncHandler<AsyncResult<T>> topHandler) {
        this(session, commands, topHandler, null);
    }

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands, AsyncHandler<AsyncResult<T>> topHandler,
            ReplicationResultHandler<T> replicationResultHandler) {
        super(session.n, topHandler);
        // w = n / 2 + 1;
        w = n; // 使用Write all read one模式

        // 手动提交事务的场景不用执行副本提交
        if (!session.isAutoCommit())
            commands = null;
        this.commands = commands;
        this.replicationResultHandler = replicationResultHandler;
    }

    @Override
    synchronized void response(AsyncResult<T> result) {
        results.add(result);
        if (!successful && results.size() >= w) {
            successful = true;
            try {
                AsyncResult<T> ar = null;
                if (replicationResultHandler != null) {
                    T ret = replicationResultHandler.handleResults(getResults());
                    ar = new AsyncResult<>();
                    ar.setResult(ret);
                }
                if (commands != null) {
                    for (ReplicaCommand c : commands) {
                        c.replicaCommit(-1, true);
                    }
                }

                if (topHandler != null) {
                    if (ar != null)
                        topHandler.handle(ar);
                    else
                        topHandler.handle(results.get(0));
                }
            } finally {
                signal();
            }
        }
    }

    @Override
    boolean isRead() {
        return false;
    }

    @Override
    int totalBlockFor() {
        return w;
    }

    ArrayList<T> getResults() {
        int size = results.size();
        ArrayList<T> results2 = new ArrayList<>(results.size());
        for (int i = 0; i < size; i++)
            results2.add(results.get(i).getResult());
        return results2;
    }
}
