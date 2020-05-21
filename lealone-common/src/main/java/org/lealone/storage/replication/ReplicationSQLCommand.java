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

import java.util.HashSet;
import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.PageKey;

class ReplicationSQLCommand extends ReplicationCommand<ReplicaSQLCommand> implements SQLCommand {

    ReplicationSQLCommand(ReplicationSession session, ReplicaSQLCommand[] commands) {
        super(session, commands);
    }

    @Override
    public int getType() {
        return REPLICATION_SQL_COMMAND;
    }

    @Override
    public List<? extends CommandParameter> getParameters() {
        return commands[0].getParameters();
    }

    @Override
    public Result getMetaData() {
        return commands[0].getMetaData();
    }

    @Override
    public boolean isQuery() {
        return commands[0].isQuery();
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        AsyncCallback<Result> ac = new AsyncCallback<>();
        HashSet<ReplicaSQLCommand> seen = new HashSet<>();
        executeQuery(maxRows, scrollable, 1, seen, ac);
        return ac;
    }

    private void executeQuery(int maxRows, boolean scrollable, int tries, HashSet<ReplicaSQLCommand> seen,
            AsyncCallback<Result> ac) {
        AsyncHandler<AsyncResult<Result>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                executeQuery(maxRows, scrollable, tries + 1, seen, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        ReadResponseHandler<Result> readResponseHandler = new ReadResponseHandler<>(session, handler);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < session.r; i++) {
            ReplicaSQLCommand c = getRandomNode(seen);
            if (c != null)
                c.executeQuery(maxRows, scrollable).onComplete(readResponseHandler);
        }
    }

    @Override
    public Future<Integer> executeUpdate(List<PageKey> pageKeys) {
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        executeUpdate(1, ac);
        return ac;
    }

    private void executeUpdate(int tries, AsyncCallback<Integer> ac) {
        String rn = session.createReplicationName();
        AsyncHandler<AsyncResult<Integer>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                executeUpdate(tries + 1, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        WriteResponseHandler<Integer> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaUpdate(rn).onComplete(writeResponseHandler);
        }
    }
}
