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
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;
import org.lealone.storage.replication.exceptions.WriteFailureException;
import org.lealone.storage.replication.exceptions.WriteTimeoutException;

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
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        HashSet<ReplicaStorageCommand> seen = new HashSet<>();
        AsyncCallback<Object> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        ReadResponseHandler<Object> readResponseHandler = new ReadResponseHandler<>(n, handler);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            ReplicaStorageCommand c = getRandomNode(seen);
            c.get(mapName, key).onComplete(readResponseHandler);
        }

        int tries = 1;
        while (true) {
            try {
                readResponseHandler.getResult(session.rpcTimeoutMillis);
                return ac;
            } catch (ReadTimeoutException | ReadFailureException e) {
                if (tries++ < session.maxTries) {
                    ReplicaStorageCommand c = getRandomNode(seen);
                    if (c != null) {
                        c.get(mapName, key).onComplete(readResponseHandler);
                        continue;
                    }
                }
                readResponseHandler.initCause(e);
                throw e;
            }
        }
    }

    @Override
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent) {
        return executePut(mapName, key, value, raw, addIfAbsent, 1);
    }

    private Future<Object> executePut(String mapName, ByteBuffer key, ByteBuffer value, boolean raw,
            boolean addIfAbsent, int tries) {
        String rn = session.createReplicationName();
        AsyncCallback<Object> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaPut(rn, mapName, key.slice(), value.slice(), raw, addIfAbsent)
                    .onComplete(writeResponseHandler);
        }
        try {
            writeResponseHandler.getResult(session.rpcTimeoutMillis);
            return ac;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxTries) {
                key.rewind();
                value.rewind();
                return executePut(mapName, key, value, raw, addIfAbsent, ++tries);
            } else {
                writeResponseHandler.initCause(e);
                throw e;
            }
        }
    }

    @Override
    public Future<Object> append(String mapName, ByteBuffer value) {
        return executeAppend(mapName, value, 1);
    }

    private Future<Object> executeAppend(String mapName, ByteBuffer value, int tries) {
        String rn = session.createReplicationName();
        AsyncCallback<Object> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            commands[i].executeReplicaAppend(rn, mapName, value.slice()).onComplete(writeResponseHandler);
        }

        try {
            writeResponseHandler.getResult(session.rpcTimeoutMillis);
            return ac;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxTries) {
                value.rewind();
                return executeAppend(mapName, value, ++tries).onComplete(writeResponseHandler);
            } else {
                writeResponseHandler.initCause(e);
                throw e;
            }
        }
    }

    @Override
    public Future<Boolean> replace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue) {
        return executeReplace(mapName, key, oldValue, newValue, 1);
    }

    private Future<Boolean> executeReplace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue,
            int tries) {
        String rn = session.createReplicationName();
        AsyncCallback<Boolean> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Boolean>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        WriteResponseHandler<Boolean> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaReplace(rn, mapName, key.slice(), oldValue.slice(), newValue.slice())
                    .onComplete(writeResponseHandler);
        }
        try {
            writeResponseHandler.getResult(session.rpcTimeoutMillis);
            return ac;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxTries) {
                key.rewind();
                oldValue.rewind();
                newValue.rewind();
                return executeReplace(mapName, key, oldValue, newValue, ++tries);
            } else {
                writeResponseHandler.initCause(e);
                throw e;
            }
        }
    }

    @Override
    public Future<Object> remove(String mapName, ByteBuffer key) {
        return executeRemove(mapName, key, 1);
    }

    private Future<Object> executeRemove(String mapName, ByteBuffer key, int tries) {
        String rn = session.createReplicationName();
        AsyncCallback<Object> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<Object>> handler = ar -> {
            ac.setAsyncResult(ar);
        };
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(session, commands, handler);

        for (int i = 0; i < session.n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaRemove(rn, mapName, key.slice()).onComplete(writeResponseHandler);
        }
        try {
            writeResponseHandler.getResult(session.rpcTimeoutMillis);
            return ac;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxTries) {
                key.rewind();
                return executeRemove(mapName, key, ++tries);
            } else {
                writeResponseHandler.initCause(e);
                throw e;
            }
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
            return plan;
        };
        WriteResponseHandler<LeafPageMovePlan> writeResponseHandler = new WriteResponseHandler<>(session, commands,
                topHandler, replicationResultHandler);

        for (int i = 0; i < n; i++) {
            commands[i].prepareMoveLeafPage(mapName, leafPageMovePlan).onComplete(writeResponseHandler);
        }
        try {
            writeResponseHandler.await(session.rpcTimeoutMillis);
            List<LeafPageMovePlan> plans = writeResponseHandler.getResults();
            LeafPageMovePlan plan = getValidPlan(plans, n);
            if (plan == null && --tries > 0) {
                leafPageMovePlan.incrementIndex();
                prepareMoveLeafPage(mapName, leafPageMovePlan, tries, topHandler);
            }
        } catch (WriteTimeoutException | WriteFailureException e) {
            writeResponseHandler.initCause(e);
            throw e;
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
