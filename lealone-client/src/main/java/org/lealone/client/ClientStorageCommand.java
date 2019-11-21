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
package org.lealone.client;

import java.nio.ByteBuffer;

import org.lealone.db.Session;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.value.ValueLong;
import org.lealone.net.AsyncCallback;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.storage.replication.ReplicationResult;

public class ClientStorageCommand implements ReplicaStorageCommand {

    private final ClientSession session;

    public ClientStorageCommand(ClientSession session) {
        this.session = session;
    }

    @Override
    public int getType() {
        return CLIENT_STORAGE_COMMAND;
    }

    @Override
    public Object put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw,
            AsyncHandler<AsyncResult<Object>> handler) {
        return executeReplicaPut(null, mapName, key, value, raw, handler);
    }

    @Override
    public Object executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw, AsyncHandler<AsyncResult<Object>> handler) {
        byte[] bytes = null;
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            if (isDistributed) {
                session.traceOperation("COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT);
            } else if (replicationName != null) {
                session.traceOperation("COMMAND_STORAGE_REPLICATION_PUT", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_REPLICATION_PUT);
            } else {
                session.traceOperation("COMMAND_STORAGE_PUT", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_PUT);
            }
            out.writeString(mapName).writeByteBuffer(key).writeByteBuffer(value);
            out.writeString(replicationName).writeBoolean(raw);

            bytes = getResult(packetId, out, handler, new AsyncCallback<byte[]>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(in.readString());
                    setResult(in.readBytes());
                }
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return bytes;
    }

    @Override
    public Object get(String mapName, ByteBuffer key, AsyncHandler<AsyncResult<Object>> handler) {
        byte[] bytes = null;
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            if (isDistributed) {
                session.traceOperation("COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET);
            } else {
                session.traceOperation("COMMAND_STORAGE_GET", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_GET);
            }
            out.writeString(mapName).writeByteBuffer(key);

            bytes = getResult(packetId, out, handler, new AsyncCallback<byte[]>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(in.readString());
                    setResult(in.readBytes());
                }
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return bytes;
    }

    @Override
    public Object append(String mapName, ByteBuffer value, ReplicationResult replicationResult,
            AsyncHandler<AsyncResult<Object>> handler) {
        return executeReplicaAppend(null, mapName, value, replicationResult, handler);
    }

    @Override
    public Object executeReplicaAppend(String replicationName, String mapName, ByteBuffer value,
            ReplicationResult replicationResult, AsyncHandler<AsyncResult<Object>> handler) {
        Long result = null;
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            if (isDistributed) {
                session.traceOperation("COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND);
            } else {
                session.traceOperation("COMMAND_STORAGE_APPEND", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_APPEND);
            }
            out.writeString(mapName).writeByteBuffer(value);
            out.writeString(replicationName);

            result = getResult(packetId, out, handler, new AsyncCallback<Long>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(in.readString());
                    setResult(in.readLong());
                }
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        replicationResult.addResult(this, result);
        return ValueLong.get(result);
    }

    @Override
    public void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            session.traceOperation("COMMAND_STORAGE_MOVE_LEAF_PAGE", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_MOVE_LEAF_PAGE);
            out.writeString(mapName).writePageKey(pageKey).writeByteBuffer(page).writeBoolean(addPage);
            out.flush();
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            session.traceOperation("COMMAND_STORAGE_REPLICATE_ROOT_PAGES", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_REPLICATE_ROOT_PAGES);
            out.writeString(dbName).writeByteBuffer(rootPages);
            out.flush();
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            session.traceOperation("COMMAND_STORAGE_REMOVE_LEAF_PAGE", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE);
            out.writeString(mapName).writePageKey(pageKey);
            out.flush();
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan,
            AsyncHandler<AsyncResult<LeafPageMovePlan>> handler) {
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            session.traceOperation("COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE);
            out.writeString(mapName);
            leafPageMovePlan.serialize(out);

            return getResult(packetId, out, handler, new AsyncCallback<LeafPageMovePlan>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    setResult(LeafPageMovePlan.deserialize(in));
                }
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public ByteBuffer readRemotePage(String mapName, PageKey pageKey, AsyncHandler<AsyncResult<ByteBuffer>> handler) {
        int packetId = session.getNextId();
        TransferOutputStream out = session.newOut();
        try {
            session.traceOperation("COMMAND_STORAGE_READ_PAGE", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_STORAGE_READ_PAGE);
            out.writeString(mapName).writePageKey(pageKey);

            return getResult(packetId, out, handler, new AsyncCallback<ByteBuffer>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    result = in.readByteBuffer();
                }
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    private static <T> T getResult(int packetId, TransferOutputStream out, AsyncHandler<?> handler, AsyncCallback<T> ac)
            throws Exception {
        if (handler != null) {
            ac.setAsyncHandler(handler);
            out.flush(packetId, ac);
            return null;
        } else {
            return out.flushAndAwait(packetId, ac);
        }
    }
}
