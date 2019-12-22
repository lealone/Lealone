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

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.value.ValueLong;
import org.lealone.server.protocol.StorageAppend;
import org.lealone.server.protocol.StorageAppendAck;
import org.lealone.server.protocol.StorageGet;
import org.lealone.server.protocol.StorageGetAck;
import org.lealone.server.protocol.StorageMoveLeafPage;
import org.lealone.server.protocol.StoragePrepareMoveLeafPage;
import org.lealone.server.protocol.StoragePrepareMoveLeafPageAck;
import org.lealone.server.protocol.StoragePut;
import org.lealone.server.protocol.StoragePutAck;
import org.lealone.server.protocol.StorageReadPage;
import org.lealone.server.protocol.StorageReadPageAck;
import org.lealone.server.protocol.StorageRemoveLeafPage;
import org.lealone.server.protocol.StorageReplicateRootPages;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicaStorageCommand;

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
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StoragePut packet = new StoragePut(mapName, key, value, isDistributed, replicationName, raw);
            if (handler != null) {
                session.<StoragePutAck> sendAsync(packet, ack -> {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    handler.handle(new AsyncResult<>(ack.result));
                });
            } else {
                StorageAppendAck ack = session.sendSync(packet);
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Object get(String mapName, ByteBuffer key, AsyncHandler<AsyncResult<Object>> handler) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageGet packet = new StorageGet(mapName, key, isDistributed);
            if (handler != null) {
                session.<StorageGetAck> sendAsync(packet, ack -> {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    handler.handle(new AsyncResult<>(ack.result));
                });
            } else {
                StorageGetAck ack = session.sendSync(packet);
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Object append(String mapName, ByteBuffer value, AsyncHandler<AsyncResult<Object>> handler) {
        return executeReplicaAppend(null, mapName, value, handler);
    }

    @Override
    public Object executeReplicaAppend(String replicationName, String mapName, ByteBuffer value,
            AsyncHandler<AsyncResult<Object>> handler) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageAppend packet = new StorageAppend(mapName, value, isDistributed, replicationName);
            if (handler != null) {
                session.<StorageAppendAck> sendAsync(packet, ack -> {
                    if (isDistributed)
                        session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    handler.handle(new AsyncResult<>(ack.result));
                });
            } else {
                StorageAppendAck ack = session.sendSync(packet);
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ValueLong.get(ack.result);
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        try {
            session.sendAsync(new StorageMoveLeafPage(mapName, pageKey, page, addPage));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        try {
            session.sendAsync(new StorageReplicateRootPages(dbName, rootPages));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        try {
            session.sendAsync(new StorageRemoveLeafPage(mapName, pageKey));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan,
            AsyncHandler<AsyncResult<LeafPageMovePlan>> handler) {
        try {
            StoragePrepareMoveLeafPage packet = new StoragePrepareMoveLeafPage(mapName, leafPageMovePlan);
            if (handler != null) {
                session.<StoragePrepareMoveLeafPageAck> sendAsync(packet, ack -> {
                    handler.handle(new AsyncResult<>(ack.leafPageMovePlan));
                });
            } else {
                StoragePrepareMoveLeafPageAck ack = session.sendSync(packet);
                return ack.leafPageMovePlan;
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public ByteBuffer readRemotePage(String mapName, PageKey pageKey, AsyncHandler<AsyncResult<ByteBuffer>> handler) {
        try {
            StorageReadPage packet = new StorageReadPage(mapName, pageKey);
            if (handler != null) {
                session.<StorageReadPageAck> sendAsync(packet, ack -> {
                    handler.handle(new AsyncResult<>(ack.page));
                });
            } else {
                StorageReadPageAck ack = session.sendSync(packet);
                return ack.page;
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }
}
