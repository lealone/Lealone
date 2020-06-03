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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

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
        final String rn = session.createReplicationName();
        ReplicationResultHandler<ReplicationUpdateAck> replicationResultHandler = results -> {
            return handleReplicationConflict(rn, results);
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

        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaUpdate(rn).onComplete(writeResponseHandler);
        }
    }

    private ReplicationUpdateAck handleReplicationConflict(String replicationName,
            List<ReplicationUpdateAck> ackResults) {
        ReplicationConflictType replicationConflictType = ReplicationConflictType.NONE;
        for (ReplicationUpdateAck ack : ackResults) {
            if (ack.replicationConflictType != ReplicationConflictType.NONE) {
                replicationConflictType = ack.replicationConflictType;
                break;
            }
        }

        switch (replicationConflictType) {
        case ROW_LOCK: {
            return handleRowLockConflict(replicationName, ackResults);
        }
        case DB_OBJECT_LOCK: {
            return handleDbObjectLockConflict(replicationName, ackResults);
        }
        case APPEND: {
            long minKey = Long.MAX_VALUE;
            for (ReplicationUpdateAck ack : ackResults) {
                ack.uncommittedReplicationNames.add(replicationName);
                if (ack.first < minKey) {
                    minKey = ack.first;
                }
            }
            return handleAppendConflict(replicationName, ackResults, minKey);
        }
        case NONE:
        default:
            boolean autoCommit = session.isAutoCommit();
            for (ReplicationUpdateAck ack : ackResults) {
                ack.getReplicaCommand().replicaCommit(-1, autoCommit);
            }
            return ackResults.get(0);
        }
    }

    private ReplicationUpdateAck handleAppendConflict(String replicationName, List<ReplicationUpdateAck> ackResults,
            long minKey) {
        TreeSet<String> uncommittedReplicationNames = new TreeSet<>();
        for (ReplicationUpdateAck ack : ackResults) {
            uncommittedReplicationNames.add(ack.uncommittedReplicationNames.get(0));
        }
        boolean found = false;
        long validKey = minKey;
        for (String name : uncommittedReplicationNames) {
            if (replicationName.equals(name)) {
                found = true;
                break;
            } else {
                validKey++;
            }
        }
        if (found) {
            boolean autoCommit = session.isAutoCommit();
            for (ReplicationUpdateAck ack : ackResults) {
                ack.getReplicaCommand().replicaCommit(validKey, autoCommit);
            }
            return new ReplicationUpdateAck(1, validKey, validKey, null, null);
        }

        for (ReplicationUpdateAck ack : ackResults) {
            if (ack.uncommittedReplicationNames != null) {
                ArrayList<String> names = new ArrayList<>(ack.uncommittedReplicationNames);
                ack.uncommittedReplicationNames.clear();
                for (int i = 0, size = names.size(); i < size; i++) {
                    String name = names.get(i);
                    if (!uncommittedReplicationNames.contains(name))
                        ack.uncommittedReplicationNames.add(name);
                }
            }
        }
        int uncommittedNodes = uncommittedReplicationNames.size();
        return handleAppendConflict(replicationName, ackResults, minKey + uncommittedNodes - 1);
    }

    private ReplicationUpdateAck handleRowLockConflict(String replicationName, List<ReplicationUpdateAck> ackResults) {
        return handleLockConflict(replicationName, ackResults, -4, -5);
    }

    private ReplicationUpdateAck handleDbObjectLockConflict(String replicationName,
            List<ReplicationUpdateAck> ackResults) {
        return handleLockConflict(replicationName, ackResults, -2, -3);
    }

    private ReplicationUpdateAck handleLockConflict(String replicationName, List<ReplicationUpdateAck> ackResults,
            long validKeyForSelf, long validKeyForOther) {
        HashMap<String, AtomicInteger> groupResults = new HashMap<>(1);
        for (ReplicationUpdateAck ack : ackResults) {
            if (ack.uncommittedReplicationNames.isEmpty())
                ack.uncommittedReplicationNames.add(replicationName);
            String name = ack.uncommittedReplicationNames.get(0);
            AtomicInteger counter = groupResults.get(name);
            if (counter == null) {
                counter = new AtomicInteger();
                groupResults.put(name, counter);
            }
            counter.incrementAndGet();
        }

        int w = session.w;
        String validReplicationName = null;
        for (Entry<String, AtomicInteger> e : groupResults.entrySet()) {
            AtomicInteger counter = e.getValue();
            if (counter.get() >= w) {
                validReplicationName = e.getKey();
                break;
            }
        }
        if (validReplicationName == null) {
            if (ackResults.size() == session.n) {
                TreeSet<String> uncommittedReplicationNames = new TreeSet<>();
                for (ReplicationUpdateAck ack : ackResults) {
                    for (String name : ack.uncommittedReplicationNames) {
                        uncommittedReplicationNames.add(name);
                    }
                }
                validReplicationName = uncommittedReplicationNames.first();
            }
        }
        if (validReplicationName != null) {
            boolean autoCommit = session.isAutoCommit();
            if (validReplicationName.equals(replicationName)) {
                for (ReplicationUpdateAck ack : ackResults) {
                    ack.getReplicaCommand().replicaCommit(validKeyForSelf, autoCommit);
                }
            } else {
                for (ReplicationUpdateAck ack : ackResults) {
                    ack.getReplicaCommand().replicaCommit(validKeyForOther, autoCommit);
                }
            }
        }
        return ackResults.get(0);
    }
}
