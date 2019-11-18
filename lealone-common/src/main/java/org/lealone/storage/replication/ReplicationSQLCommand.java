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
import java.util.HashSet;
import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.result.Result;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;
import org.lealone.storage.replication.exceptions.WriteFailureException;
import org.lealone.storage.replication.exceptions.WriteTimeoutException;

public class ReplicationSQLCommand extends ReplicationCommand<SQLCommand> implements SQLCommand {

    public ReplicationSQLCommand(ReplicationSession session, SQLCommand[] commands) {
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
        return executeQuery(maxRows, false);
    }

    @Override
    public Result executeQuery(final int maxRows, final boolean scrollable) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        final HashSet<SQLCommand> seen = new HashSet<>();
        final ReadResponseHandler readResponseHandler = new ReadResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            final SQLCommand c = getRandomNode(seen);
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    Result result = null;
                    try {
                        result = c.executeQuery(maxRows, scrollable);
                        readResponseHandler.response(result);
                    } catch (Exception e) {
                        exceptions.add(e);
                        if (readResponseHandler != null) {
                            readResponseHandler.onFailure();
                            SQLCommand c = getRandomNode(seen);
                            if (c != null) {
                                result = c.executeQuery(maxRows, scrollable);
                                readResponseHandler.response(result);
                                return;
                            }
                        }
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            return readResponseHandler.get(session.rpcTimeoutMillis);
        } catch (ReadTimeoutException | ReadFailureException e) {
            if (!exceptions.isEmpty())
                e.initCause(exceptions.get(0));
            throw e;
        }
    }

    @Override
    public int executeUpdate() {
        return executeUpdate(1);
    }

    @Override
    public int executeReplicaUpdate(String replicationName, ReplicationResult replicationResult) {
        return executeUpdate();
    }

    private int executeUpdate(int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);
        final ReplicationResult replicationResult = new ReplicationResult(session.n, session.w,
                session.isAutoCommit(), this.commands);

        for (int i = 0; i < n; i++) {
            final SQLCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executeReplicaUpdate(rn, replicationResult));
                    } catch (Exception e) {
                        writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            writeResponseHandler.getUpdateCount(session.rpcTimeoutMillis);
            replicationResult.validate();
            return replicationResult.getUpdateCount();
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries)
                return executeUpdate(++tries);
            else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }
}
