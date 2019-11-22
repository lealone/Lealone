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
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.sql.SQLCommand;
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
    public Result executeQuery(int maxRows) {
        return query(maxRows, false, null);
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        return query(maxRows, scrollable, null);
    }

    @Override
    public void executeQueryAsync(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        query(maxRows, scrollable, handler);
    }

    private Result query(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        HashSet<ReplicaSQLCommand> seen = new HashSet<>();
        ReadResponseHandler<Result> readResponseHandler = new ReadResponseHandler<>(n, handler);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            ReplicaSQLCommand c = getRandomNode(seen);
            c.executeQueryAsync(maxRows, scrollable, readResponseHandler);
        }

        if (handler == null) {
            int tries = 1;
            while (true) {
                try {
                    return readResponseHandler.getResult(session.rpcTimeoutMillis);
                } catch (ReadTimeoutException | ReadFailureException e) {
                    if (tries++ < session.maxTries) {
                        ReplicaSQLCommand c = getRandomNode(seen);
                        if (c != null) {
                            c.executeQueryAsync(maxRows, scrollable, readResponseHandler);
                            continue;
                        }
                    }
                    readResponseHandler.initCause(e);
                    throw e;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public int executeUpdate() {
        return executeUpdate(1, null);
    }

    @Override
    public void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        executeUpdate(1, handler);
    }

    private int executeUpdate(int tries, AsyncHandler<AsyncResult<Integer>> handler) {
        int n = session.n;
        String rn = session.createReplicationName();
        ReplicationResult replicationResult = new ReplicationResult(session, commands);
        WriteResponseHandler<Integer> writeResponseHandler = new WriteResponseHandler<>(n, handler, replicationResult);

        for (int i = 0; i < n; i++) {
            commands[i].executeReplicaUpdateAsync(rn, replicationResult, writeResponseHandler);
        }
        if (handler == null) {
            try {
                return writeResponseHandler.getResult(session.rpcTimeoutMillis);
            } catch (WriteTimeoutException | WriteFailureException e) {
                if (tries < session.maxTries)
                    return executeUpdate(++tries, handler);
                else {
                    writeResponseHandler.initCause(e);
                    throw e;
                }
            }
        } else {
            return 0;
        }
    }
}
