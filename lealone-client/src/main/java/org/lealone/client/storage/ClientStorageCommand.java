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
package org.lealone.client.storage;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.client.session.ClientSession;
import org.lealone.db.async.Future;
import org.lealone.db.value.ValueLong;
import org.lealone.server.protocol.replication.ReplicationHandleReplicaConflict;
import org.lealone.server.protocol.storage.StorageAppend;
import org.lealone.server.protocol.storage.StorageAppendAck;
import org.lealone.server.protocol.storage.StorageGet;
import org.lealone.server.protocol.storage.StorageGetAck;
import org.lealone.server.protocol.storage.StorageMoveLeafPage;
import org.lealone.server.protocol.storage.StoragePrepareMoveLeafPage;
import org.lealone.server.protocol.storage.StoragePrepareMoveLeafPageAck;
import org.lealone.server.protocol.storage.StoragePut;
import org.lealone.server.protocol.storage.StoragePutAck;
import org.lealone.server.protocol.storage.StorageReadPage;
import org.lealone.server.protocol.storage.StorageReadPageAck;
import org.lealone.server.protocol.storage.StorageRemove;
import org.lealone.server.protocol.storage.StorageRemoveAck;
import org.lealone.server.protocol.storage.StorageRemoveLeafPage;
import org.lealone.server.protocol.storage.StorageReplace;
import org.lealone.server.protocol.storage.StorageReplaceAck;
import org.lealone.server.protocol.storage.StorageReplicatePages;
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
    public Future<Object> get(String mapName, ByteBuffer key) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageGet packet = new StorageGet(mapName, key, isDistributed);
            return session.<Object, StorageGetAck> send(packet, ack -> {
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent) {
        return executeReplicaPut(null, mapName, key, value, raw, addIfAbsent);
    }

    @Override
    public Future<Object> executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw, boolean addIfAbsent) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StoragePut packet = new StoragePut(mapName, key, value, isDistributed, replicationName, raw, addIfAbsent);
            return session.<Object, StoragePutAck> send(packet, ack -> {
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Future<Object> append(String mapName, ByteBuffer value) {
        return executeReplicaAppend(null, mapName, value);
    }

    @Override
    public Future<Object> executeReplicaAppend(String replicationName, String mapName, ByteBuffer value) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageAppend packet = new StorageAppend(mapName, value, isDistributed, replicationName);
            return session.<Object, StorageAppendAck> send(packet, ack -> {
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ValueLong.get(ack.result);
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Future<Boolean> replace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue) {
        return executeReplicaReplace(null, mapName, key, oldValue, newValue);
    }

    @Override
    public Future<Boolean> executeReplicaReplace(String replicationName, String mapName, ByteBuffer key,
            ByteBuffer oldValue, ByteBuffer newValue) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageReplace packet = new StorageReplace(mapName, key, oldValue, newValue, isDistributed,
                    replicationName);
            return session.<Boolean, StorageReplaceAck> send(packet, ack -> {
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Future<Object> remove(String mapName, ByteBuffer key) {
        return executeReplicaRemove(null, mapName, key);
    }

    @Override
    public Future<Object> executeReplicaRemove(String replicationName, String mapName, ByteBuffer key) {
        try {
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageRemove packet = new StorageRemove(mapName, key, isDistributed, replicationName);
            return session.<Object, StorageRemoveAck> send(packet, ack -> {
                if (isDistributed)
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.result;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        try {
            session.send(new StorageMoveLeafPage(mapName, pageKey, page, addPage));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void replicatePages(String dbName, String storageName, ByteBuffer pages) {
        try {
            session.send(new StorageReplicatePages(dbName, storageName, pages));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        try {
            session.send(new StorageRemoveLeafPage(mapName, pageKey));
        } catch (Exception e) {
            session.handleException(e);
        }
    }

    @Override
    public Future<LeafPageMovePlan> prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        try {
            StoragePrepareMoveLeafPage packet = new StoragePrepareMoveLeafPage(mapName, leafPageMovePlan);
            return session.<LeafPageMovePlan, StoragePrepareMoveLeafPageAck> send(packet, ack -> {
                return ack.leafPageMovePlan;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public Future<ByteBuffer> readRemotePage(String mapName, PageKey pageKey) {
        try {
            StorageReadPage packet = new StorageReadPage(mapName, pageKey);
            return session.<ByteBuffer, StorageReadPageAck> send(packet, ack -> {
                return ack.page;
            });
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    public void handleReplicaConflict(List<String> retryReplicationNames) {
        try {
            session.send(new ReplicationHandleReplicaConflict(retryReplicationNames));
        } catch (Exception e) {
            session.getTrace().error(e, "handleReplicaConflict");
        }
    }
}
