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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.replication.WriteResponseHandler.ReplicationResultHandler;

class ReplicationStorageCommand extends ReplicationCommand<ReplicaStorageCommand> implements StorageCommand {

    ReplicationStorageCommand(ReplicationSession session, ReplicaStorageCommand[] commands) {
        super(session, commands);
    }

    @Override
    public int getType() {
        return REPLICATION_STORAGE_COMMAND;
    }

    @Override
    public Future<Object> get(String mapName, ByteBuffer key) {
        HashSet<ReplicaStorageCommand> seen = new HashSet<>();
        AsyncCallback<Object> ac = new AsyncCallback<>();
        get(mapName, key, 1, seen, ac);
        return ac;
    }

    public void get(String mapName, ByteBuffer key, int tries, HashSet<ReplicaStorageCommand> seen,
            AsyncCallback<Object> ac) {
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                key.rewind();
                get(mapName, key, tries + 1, seen, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        ReadResponseHandler<Object> readResponseHandler = new ReadResponseHandler<>(session, handler);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < session.r; i++) {
            ReplicaStorageCommand c = getRandomNode(seen);
            c.get(mapName, key.slice()).onComplete(readResponseHandler);
        }
    }

    @Override
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent) {
        AsyncCallback<Object> ac = new AsyncCallback<>();
        executePut(mapName, key, value, raw, addIfAbsent, 1, ac);
        return ac;
    }

    private void executePut(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent,
            int tries, AsyncCallback<Object> ac) {
        String rn = session.createReplicationName();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                key.rewind();
                value.rewind();
                executePut(mapName, key, value, raw, addIfAbsent, tries + 1, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaPut(rn, mapName, key.slice(), value.slice(), raw, addIfAbsent)
                    .onComplete(writeResponseHandler);
        }
    }

    @Override
    public Future<Object> append(String mapName, ByteBuffer value) {
        AsyncCallback<Object> ac = new AsyncCallback<>();
        executeAppend(mapName, value, 1, ac);
        return ac;
    }

    private void executeAppend(String mapName, ByteBuffer value, int tries, AsyncCallback<Object> ac) {
        String rn = session.createReplicationName();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                value.rewind();
                executeAppend(mapName, value, tries + 1, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaAppend(rn, mapName, value.slice()).onComplete(writeResponseHandler);
        }
    }

    @Override
    public Future<Boolean> replace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue) {
        AsyncCallback<Boolean> ac = new AsyncCallback<>();
        executeReplace(mapName, key, oldValue, newValue, 1, ac);
        return ac;
    }

    private void executeReplace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue, int tries,
            AsyncCallback<Boolean> ac) {
        String rn = session.createReplicationName();
        AsyncHandler<AsyncResult<Boolean>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                key.rewind();
                oldValue.rewind();
                newValue.rewind();
                executeReplace(mapName, key, oldValue, newValue, tries + 1, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        WriteResponseHandler<Boolean> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaReplace(rn, mapName, key.slice(), oldValue.slice(), newValue.slice())
                    .onComplete(writeResponseHandler);
        }
    }

    @Override
    public Future<Object> remove(String mapName, ByteBuffer key) {
        AsyncCallback<Object> ac = new AsyncCallback<>();
        executeRemove(mapName, key, 1, ac);
        return ac;
    }

    private void executeRemove(String mapName, ByteBuffer key, int tries, AsyncCallback<Object> ac) {
        String rn = session.createReplicationName();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            if (ar.isFailed() && tries < session.maxTries) {
                key.rewind();
                executeRemove(mapName, key, tries + 1, ac);
            } else {
                ac.setAsyncResult(ar);
            }
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaRemove(rn, mapName, key.slice()).onComplete(writeResponseHandler);
        }
    }

    @Override
    public Future<LeafPageMovePlan> prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        AsyncCallback<LeafPageMovePlan> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<LeafPageMovePlan>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        prepareMoveLeafPage(mapName, leafPageMovePlan, 3, handler);
        return ac;
    }

    private void prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan, int tries,
            AsyncHandler<AsyncResult<LeafPageMovePlan>> topHandler) {
        int n = session.n;
        ReplicationResultHandler<LeafPageMovePlan> replicationResultHandler = results -> {
            LeafPageMovePlan plan = getValidPlan(results, n);
            if (plan == null && tries - 1 > 0) {
                leafPageMovePlan.incrementIndex();
                prepareMoveLeafPage(mapName, leafPageMovePlan, tries + 1, topHandler);
            }
            return plan;
        };
        WriteResponseHandler<LeafPageMovePlan> writeResponseHandler = new WriteResponseHandler<>(session, commands,
                topHandler, replicationResultHandler);

        for (int i = 0; i < n; i++) {
            commands[i].prepareMoveLeafPage(mapName, leafPageMovePlan).onComplete(writeResponseHandler);
        }
    }

    private LeafPageMovePlan getValidPlan(List<LeafPageMovePlan> plans, int n) {
        HashMap<String, ArrayList<LeafPageMovePlan>> groupPlans = new HashMap<>(1);
        for (LeafPageMovePlan p : plans) {
            ArrayList<LeafPageMovePlan> group = groupPlans.get(p.moverHostId);
            if (group == null) {
                group = new ArrayList<>(n);
                groupPlans.put(p.moverHostId, group);
            }
            group.add(p);
        }
        int w = n / 2 + 1;
        LeafPageMovePlan validPlan = null;
        for (Entry<String, ArrayList<LeafPageMovePlan>> e : groupPlans.entrySet()) {
            ArrayList<LeafPageMovePlan> group = e.getValue();
            if (group.size() >= w) {
                validPlan = group.get(0);
                break;
            }
        }
        return validPlan;
    }

    @Override
    public void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        for (int i = 0, n = session.n; i < n; i++) {
            commands[i].moveLeafPage(mapName, pageKey, page.slice(), addPage);
        }
    }

    @Override
    public void replicatePages(String dbName, String storageName, ByteBuffer pages) {
        for (int i = 0, n = session.n; i < n; i++) {
            commands[i].replicatePages(dbName, storageName, pages.slice());
        }
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        for (int i = 0, n = session.n; i < n; i++) {
            commands[i].removeLeafPage(mapName, pageKey);
        }
    }

    @Override
    public Future<ByteBuffer> readRemotePage(String mapName, PageKey pageKey) {
        return commands[0].readRemotePage(mapName, pageKey);
    }
}
