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

    private final ReplicaCommand[] commands;
    private final ReplicationResultHandler<T> replicationResultHandler;
    private volatile boolean end;

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands,
            AsyncHandler<AsyncResult<T>> finalResultHandler) {
        this(session, commands, finalResultHandler, null);
    }

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands,
            AsyncHandler<AsyncResult<T>> finalResultHandler, ReplicationResultHandler<T> replicationResultHandler) {
        super(session.n, session.w, finalResultHandler);

        // 手动提交事务的场景不用执行副本提交
        if (!session.isAutoCommit())
            commands = null;
        this.commands = commands;
        this.replicationResultHandler = replicationResultHandler;
    }

    @Override
    void onSuccess() {
        AsyncResult<T> ar = null;
        if (replicationResultHandler != null) {
            T ret = replicationResultHandler.handleResults(getResults());
            ar = new AsyncResult<>(ret);
        } else {
            ar = results.get(0);
        }
        if (commands != null) {
            for (ReplicaCommand c : commands) {
                c.handleReplicaConflict(null);
            }
        }
        if (!end) {
            end = ar != null && ar.getResult() != null;
            if (finalResultHandler != null)
                finalResultHandler.handle(ar);
        }
    }

    private ArrayList<T> getResults() {
        int size = results.size();
        ArrayList<T> results2 = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            results2.add(results.get(i).getResult());
        return results2;
    }
}
