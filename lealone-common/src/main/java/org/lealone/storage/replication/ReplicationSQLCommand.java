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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.CommandParameter;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.WriteResponseHandler.ReplicationResultHandler;

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
        ReplicationResultHandler<ReplicationUpdateAck> replicationResultHandler = results -> {
            return handleReplicationConflict(results);
        };
        AsyncHandler<AsyncResult<ReplicationUpdateAck>> finalResultHandler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                executeUpdate(tries + 1, ac);
            } else {
                ac.setAsyncResult(new AsyncResult<>(ar.getResult().updateCount));
            }
        };
        // commands参数设为null，在handleReplicationConflict中处理提交或回滚
        WriteResponseHandler<ReplicationUpdateAck> writeResponseHandler = new WriteResponseHandler<>(session, null,
                finalResultHandler, replicationResultHandler);

        String rn = session.createReplicationName();
        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaUpdate(rn).onComplete(writeResponseHandler);
        }
    }

    private ReplicationUpdateAck handleReplicationConflict(List<ReplicationUpdateAck> ackResults) {
        if (ackResults.size() > 0 && ackResults.get(0).key < 0) {
            for (ReplicaCommand c : commands) {
                c.replicaCommit(-1, true);
            }
            return ackResults.get(0);
        }

        HashMap<ReplicationUpdateAck, AtomicLong> results = new HashMap<>(commands.length);
        for (ReplicationUpdateAck ack : ackResults) {
            results.put(ack, new AtomicLong(ack.key));
        }
        final int n = session.n; // 复制集群节点总个数
        final int w = session.w; // 写成功的最少节点个数
        final boolean autoCommit = session.isAutoCommit();

        HashMap<Long, ArrayList<ReplicationUpdateAck>> groupResults = new HashMap<>(1);
        for (Entry<ReplicationUpdateAck, AtomicLong> e : results.entrySet()) {
            long v = e.getValue().get();
            if (v != -1) {
                ArrayList<ReplicationUpdateAck> group = groupResults.get(v);
                if (group == null) {
                    group = new ArrayList<>(n);
                    groupResults.put(v, group);
                }
                group.add(e.getKey());
            }
        }
        boolean successful = false;
        long validKey = -1;
        ArrayList<ReplicationUpdateAck> validNodes = null;
        ArrayList<ReplicationUpdateAck> invalidNodes = new ArrayList<>(n);
        for (Entry<Long, ArrayList<ReplicationUpdateAck>> e : groupResults.entrySet()) {
            ArrayList<ReplicationUpdateAck> nodes = e.getValue();
            if (nodes.size() >= w) {
                successful = true;
                validKey = e.getKey();
                validNodes = e.getValue();
            } else {
                invalidNodes.addAll(nodes);
            }
        }
        if (successful) {
            if (validNodes.size() == n) {
                for (ReplicationUpdateAck ack : results.keySet()) {
                    ack.getReplicaCommand().replicaCommit(-1, autoCommit);
                }
            } else {
                if (validNodes != null) {
                    for (ReplicationUpdateAck ack : validNodes) {
                        ack.getReplicaCommand().replicaCommit(-1, autoCommit);
                    }
                }
                for (ReplicationUpdateAck ack : invalidNodes) {
                    ack.getReplicaCommand().replicaCommit(validKey, autoCommit);
                }
            }
            return validNodes.get(0);
        } else {
            for (ReplicationUpdateAck ack : results.keySet()) {
                ack.getReplicaCommand().replicaRollback();
            }
            return null;
        }
    }
}
