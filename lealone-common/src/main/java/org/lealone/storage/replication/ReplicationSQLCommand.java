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
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;
import org.lealone.storage.replication.exceptions.WriteFailureException;
import org.lealone.storage.replication.exceptions.WriteTimeoutException;

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
    public Future<Result> executeQuery(int maxRows) {
        return query(maxRows, false);
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable) {
        return query(maxRows, scrollable);
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        return query(maxRows, scrollable);
    }

    private Future<Result> query(int maxRows, boolean scrollable) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        HashSet<ReplicaSQLCommand> seen = new HashSet<>();
        AsyncCallback<Result> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Result>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        ReadResponseHandler<Result> readResponseHandler = new ReadResponseHandler<>(n, handler);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            ReplicaSQLCommand c = getRandomNode(seen);
            c.executeQuery(maxRows, scrollable).onComplete(readResponseHandler);
        }

        int tries = 1;
        while (true) {
            try {
                readResponseHandler.getResult(session.rpcTimeoutMillis);
                return ac;
            } catch (ReadTimeoutException | ReadFailureException e) {
                if (tries++ < session.maxTries) {
                    ReplicaSQLCommand c = getRandomNode(seen);
                    if (c != null) {
                        c.executeQuery(maxRows, scrollable).onComplete(readResponseHandler);
                        continue;
                    }
                }
                readResponseHandler.initCause(e);
                throw e;
            }
        }
    }

    @Override
    public Future<Integer> executeUpdate(List<PageKey> pageKeys) {
        return executeUpdate(1);
    }

    private Future<Integer> executeUpdate(int tries) {
        String rn = session.createReplicationName();
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Integer>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        WriteResponseHandler<Integer> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaUpdate(rn).onComplete(writeResponseHandler);
        }
        try {
            writeResponseHandler.getResult(session.rpcTimeoutMillis);
            return ac;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxTries)
                return executeUpdate(++tries);
            else {
                writeResponseHandler.initCause(e);
                throw e;
            }
        }
    }
}
