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
import java.util.concurrent.Future;

import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.replication.exceptions.ReadFailureException;
import org.lealone.storage.replication.exceptions.ReadTimeoutException;
import org.lealone.storage.replication.exceptions.WriteFailureException;
import org.lealone.storage.replication.exceptions.WriteTimeoutException;

public class ReplicationStorageCommand extends ReplicationCommand<StorageCommand> implements StorageCommand {

    public ReplicationStorageCommand(ReplicationSession session, StorageCommand[] commands) {
        super(session, commands);
    }

    @Override
    public int getType() {
        return REPLICATION_STORAGE_COMMAND;
    }

    @Override
    public Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value, boolean raw) {
        return executePut(mapName, key, value, raw, 1);
    }

    private Object executePut(final String mapName, final ByteBuffer key, final ByteBuffer value, final boolean raw,
            int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);

        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executePut(rn, mapName, key.slice(), value.slice(), raw));
                    } catch (Exception e) {
                        writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            return writeResponseHandler.getResult(session.rpcTimeoutMillis);
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries) {
                key.rewind();
                value.rewind();
                return executePut(mapName, key, value, raw, ++tries);
            } else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }

    @Override
    public Object executeGet(final String mapName, final ByteBuffer key) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        final HashSet<StorageCommand> seen = new HashSet<>();
        final ReadResponseHandler readResponseHandler = new ReadResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            final StorageCommand c = getRandomNode(seen);
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    Object result = null;
                    try {
                        result = c.executeGet(mapName, key);
                        readResponseHandler.response(result);
                    } catch (Exception e) {
                        if (readResponseHandler != null) {
                            readResponseHandler.onFailure();
                            StorageCommand c = getRandomNode(seen);
                            if (c != null) {
                                result = c.executeGet(mapName, key);
                                readResponseHandler.response(result);
                                return;
                            }
                        }
                        exceptions.add(e);
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            return readResponseHandler.getResultObject(session.rpcTimeoutMillis);
        } catch (ReadTimeoutException | ReadFailureException e) {
            if (!exceptions.isEmpty())
                e.initCause(exceptions.get(0));
            throw e;
        }
    }

    @Override
    public Object executeAppend(String replicationName, String mapName, ByteBuffer value,
            ReplicationResult replicationResult) {
        return executeAppend(mapName, value, 1);
    }

    private Object executeAppend(final String mapName, final ByteBuffer value, int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);
        final ReplicationResult replicationResult = new ReplicationResult(session.n, session.w,
                session.isAutoCommit(), this.commands);

        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executeAppend(rn, mapName, value.slice(), replicationResult));
                    } catch (Exception e) {
                        writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            Object result = writeResponseHandler.getResult(session.rpcTimeoutMillis);
            replicationResult.validate();
            return result;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries) {
                value.rewind();
                return executeAppend(mapName, value, ++tries);
            } else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        return prepareMoveLeafPage(mapName, leafPageMovePlan, 3);
    }

    private LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan, int tries) {
        final int n = session.n;
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Exception> exceptions = new ArrayList<>(1);
        final ArrayList<LeafPageMovePlan> plans = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        LeafPageMovePlan plan = c.prepareMoveLeafPage(mapName, leafPageMovePlan);
                        plans.add(plan);
                        writeResponseHandler.response(plan);
                    } catch (Exception e) {
                        writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            ThreadPool.executor.submit(command);
        }

        try {
            writeResponseHandler.await(session.rpcTimeoutMillis);

            LeafPageMovePlan plan = getValidPlan(plans, n);
            if (plan == null && --tries > 0) {
                leafPageMovePlan.incrementIndex();
                return prepareMoveLeafPage(mapName, leafPageMovePlan, tries);
            }
            return plan;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (!exceptions.isEmpty())
                e.initCause(exceptions.get(0));
            throw e;
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
    public void moveLeafPage(final String mapName, final PageKey pageKey, final ByteBuffer page,
            final boolean addPage) {
        int n = session.n;
        ArrayList<Future<?>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    c.moveLeafPage(mapName, pageKey, page.slice(), addPage);
                }
            };
            futures.add(ThreadPool.executor.submit(command));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        int n = session.n;
        ArrayList<Future<?>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    c.replicateRootPages(dbName, rootPages.slice());
                }
            };
            futures.add(ThreadPool.executor.submit(command));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void removeLeafPage(final String mapName, final PageKey pageKey) {
        int n = session.n;
        ArrayList<Future<?>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final StorageCommand c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    c.removeLeafPage(mapName, pageKey);
                }
            };
            futures.add(ThreadPool.executor.submit(command));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public ByteBuffer readRemotePage(String mapName, PageKey pageKey) {
        return commands[0].readRemotePage(mapName, pageKey);
    }
}
