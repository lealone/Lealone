/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client.storage;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.client.session.ClientSession;
import org.lealone.db.DataBuffer;
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
import org.lealone.storage.type.StorageDataType;

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
    public Future<Object> get(String mapName, Object key, StorageDataType keyType) {
        try {
            DataBuffer k = DataBuffer.create();
            ByteBuffer keyBuffer = k.write(keyType, key);
            boolean isDistributed = session.getParentTransaction() != null
                    && !session.getParentTransaction().isAutoCommit();
            StorageGet packet = new StorageGet(mapName, keyBuffer, isDistributed);
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
    public Future<Object> put(String mapName, Object key, StorageDataType keyType, Object value,
            StorageDataType valueType, boolean raw, boolean addIfAbsent) {
        DataBuffer k = DataBuffer.create();
        DataBuffer v = DataBuffer.create();
        ByteBuffer keyBuffer = k.write(keyType, key);
        ByteBuffer valueBuffer = v.write(valueType, value);
        return executeReplicaPut(null, mapName, keyBuffer, valueBuffer, raw, addIfAbsent);
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
    public Future<Object> append(String mapName, Object value, StorageDataType valueType) {
        DataBuffer v = DataBuffer.create();
        ByteBuffer valueBuffer = v.write(valueType, value);
        return executeReplicaAppend(null, mapName, valueBuffer);
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
    public Future<Boolean> replace(String mapName, Object key, StorageDataType keyType, Object oldValue,
            Object newValue, StorageDataType valueType) {
        DataBuffer k = DataBuffer.create();
        DataBuffer v1 = DataBuffer.create();
        DataBuffer v2 = DataBuffer.create();
        ByteBuffer keyBuffer = k.write(keyType, key);
        ByteBuffer valueBuffer1 = v1.write(valueType, oldValue);
        ByteBuffer valueBuffer2 = v2.write(valueType, newValue);
        return executeReplicaReplace(null, mapName, keyBuffer, valueBuffer1, valueBuffer2);
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
    public Future<Object> remove(String mapName, Object key, StorageDataType keyType) {
        DataBuffer k = DataBuffer.create();
        ByteBuffer keyBuffer = k.write(keyType, key);
        return executeReplicaRemove(null, mapName, keyBuffer);
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
