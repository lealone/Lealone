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
import java.util.Map.Entry;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
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
    public Object put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw,
            AsyncHandler<AsyncResult<Object>> handler) {
        return executePut(mapName, key, value, raw, 1, handler);
    }

    private Object executePut(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, int tries,
            AsyncHandler<AsyncResult<Object>> handler) {
        int n = session.n;
        String rn = session.createReplicationName();
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(n, handler, null);

        for (int i = 0; i < n; i++) {
            ReplicaStorageCommand c = commands[i];
            c.executeReplicaPut(rn, mapName, key.slice(), value.slice(), raw, handler);
        }
        if (handler == null) {
            try {
                return writeResponseHandler.getResult(session.rpcTimeoutMillis);
            } catch (WriteTimeoutException | WriteFailureException e) {
                if (tries < session.maxRries) {
                    key.rewind();
                    value.rewind();
                    return executePut(mapName, key, value, raw, ++tries, handler);
                } else {
                    writeResponseHandler.initCause(e);
                    throw e;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public Object get(String mapName, ByteBuffer key, AsyncHandler<AsyncResult<Object>> handler) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        HashSet<ReplicaStorageCommand> seen = new HashSet<>();
        ReadResponseHandler<Object> readResponseHandler = new ReadResponseHandler<>(n, null);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            ReplicaStorageCommand c = getRandomNode(seen);
            c.get(mapName, key, handler);
        }

        if (handler == null) {
            int tries = 1;
            while (true) {
                try {
                    return readResponseHandler.getResult(session.rpcTimeoutMillis);
                } catch (ReadTimeoutException | ReadFailureException e) {
                    if (tries++ < session.maxRries) {
                        ReplicaStorageCommand c = getRandomNode(seen);
                        if (c != null) {
                            c.get(mapName, key, handler);
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
    public Object append(String mapName, ByteBuffer value, ReplicationResult replicationResult,
            AsyncHandler<AsyncResult<Object>> handler) {
        return executeAppend(mapName, value, 1, handler);
    }

    private Object executeAppend(String mapName, ByteBuffer value, int tries,
            AsyncHandler<AsyncResult<Object>> handler) {
        int n = session.n;
        String rn = session.createReplicationName();
        WriteResponseHandler<Object> writeResponseHandler = new WriteResponseHandler<>(n, null, null);
        ReplicationResult replicationResult = new ReplicationResult(session, commands);

        for (int i = 0; i < n; i++) {
            commands[i].executeReplicaAppend(rn, mapName, value.slice(), replicationResult, handler);
        }

        if (handler == null) {
            try {
                Object result = writeResponseHandler.getResult(session.rpcTimeoutMillis);
                replicationResult.validate();
                return result;
            } catch (WriteTimeoutException | WriteFailureException e) {
                if (tries < session.maxRries) {
                    value.rewind();
                    return executeAppend(mapName, value, ++tries, handler);
                } else {
                    writeResponseHandler.initCause(e);
                    throw e;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan,
            AsyncHandler<AsyncResult<LeafPageMovePlan>> handler) {
        return prepareMoveLeafPage(mapName, leafPageMovePlan, 3, handler);
    }

    private LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan, int tries,
            AsyncHandler<AsyncResult<LeafPageMovePlan>> handler) {
        int n = session.n;
        ReplicationResult replicationResult = new ReplicationResult(session, commands) {
            @Override
            public Object validate(Object results) {
                @SuppressWarnings("unchecked")
                ArrayList<LeafPageMovePlan> plans = (ArrayList<LeafPageMovePlan>) results;
                LeafPageMovePlan plan = getValidPlan(plans, n);
                AsyncResult<LeafPageMovePlan> ar = new AsyncResult<>();
                ar.setResult(plan);
                return ar;
            }
        };
        WriteResponseHandler<LeafPageMovePlan> writeResponseHandler = new WriteResponseHandler<>(n, handler,
                replicationResult);

        for (int i = 0; i < n; i++) {
            commands[i].prepareMoveLeafPage(mapName, leafPageMovePlan, handler);
        }
        if (handler == null) {
            try {
                writeResponseHandler.await(session.rpcTimeoutMillis);
                ArrayList<LeafPageMovePlan> plans = writeResponseHandler.getResults();
                LeafPageMovePlan plan = getValidPlan(plans, n);
                if (plan == null && --tries > 0) {
                    leafPageMovePlan.incrementIndex();
                    return prepareMoveLeafPage(mapName, leafPageMovePlan, tries, handler);
                }
                return plan;
            } catch (WriteTimeoutException | WriteFailureException e) {
                writeResponseHandler.initCause(e);
                throw e;
            }
        } else {
            return null;
        }
    }

    private LeafPageMovePlan getValidPlan(ArrayList<LeafPageMovePlan> plans, int n) {
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
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        for (int i = 0, n = session.n; i < n; i++) {
            commands[i].replicateRootPages(dbName, rootPages.slice());
        }
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        for (int i = 0, n = session.n; i < n; i++) {
            commands[i].removeLeafPage(mapName, pageKey);
        }
    }

    @Override
    public ByteBuffer readRemotePage(String mapName, PageKey pageKey, AsyncHandler<AsyncResult<ByteBuffer>> handler) {
        return commands[0].readRemotePage(mapName, pageKey, handler);
    }
}
