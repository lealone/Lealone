/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import java.nio.ByteBuffer;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.ValueLong;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
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
import org.lealone.storage.StorageMap;

class StoragePacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.STORAGE_GET, new Get());
        register(PacketType.STORAGE_PUT, new Put());
        register(PacketType.STORAGE_APPEND, new Append());
        register(PacketType.STORAGE_REPLACE, new Replace());
        register(PacketType.STORAGE_REMOVE, new Remove());

        register(PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE, new PrepareMoveLeafPage());
        register(PacketType.STORAGE_MOVE_LEAF_PAGE, new MoveLeafPage());
        register(PacketType.STORAGE_REPLICATE_PAGES, new ReplicatePages());
        register(PacketType.STORAGE_READ_PAGE, new ReadPage());
        register(PacketType.STORAGE_REMOVE_LEAF_PAGE, new RemoveLeafPage());
    }

    private static class Get implements PacketHandler<StorageGet> {
        @Override
        public Packet handle(ServerSession session, StorageGet packet) {
            String localTransactionNames = getLocalTransactionNames(session, packet.isDistributedTransaction);
            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            Object result = map.get(map.getKeyType().read(packet.key));
            ByteBuffer resultByteBuffer;
            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    map.getValueType().write(writeBuffer, result);
                    resultByteBuffer = writeBuffer.getAndFlipBuffer();
                }
            } else {
                resultByteBuffer = null;
            }
            return new StorageGetAck(resultByteBuffer, localTransactionNames);
        }
    }

    private static class Put implements PacketHandler<StoragePut> {
        @Override
        public Packet handle(PacketDeliveryTask task, StoragePut packet) {
            String localTransactionNames = getLocalTransactionNames(task.session, packet.isDistributedTransaction);
            task.session.createReplicaStorageCommand().executeReplicaPut(packet.replicationName, packet.mapName,
                    packet.key, packet.value, packet.raw, packet.addIfAbsent).onComplete(ar -> {
                        if (ar.isSucceeded()) {
                            ByteBuffer result = (ByteBuffer) ar.getResult();
                            task.conn.sendResponse(task, new StoragePutAck(result, localTransactionNames));
                        } else {
                            task.conn.sendError(task.session, task.packetId, ar.getCause());
                        }
                    });
            return null;
        }
    }

    private static class Append implements PacketHandler<StorageAppend> {
        @Override
        public Packet handle(ServerSession session, StorageAppend packet) {
            String localTransactionNames = getLocalTransactionNames(session, packet.isDistributedTransaction);
            Object result = session.createReplicaStorageCommand()
                    .executeReplicaAppend(packet.replicationName, packet.mapName, packet.value).get();
            return new StorageAppendAck(((ValueLong) result).getLong(), localTransactionNames);
        }
    }

    private static class Replace implements PacketHandler<StorageReplace> {
        @Override
        public Packet handle(ServerSession session, StorageReplace packet) {
            String localTransactionNames = getLocalTransactionNames(session, packet.isDistributedTransaction);
            boolean result = session.createReplicaStorageCommand().executeReplicaReplace(packet.replicationName,
                    packet.mapName, packet.key, packet.oldValue, packet.newValue).get();
            return new StorageReplaceAck(result, localTransactionNames);
        }
    }

    private static class Remove implements PacketHandler<StorageRemove> {
        @Override
        public Packet handle(ServerSession session, StorageRemove packet) {
            String localTransactionNames = getLocalTransactionNames(session, packet.isDistributedTransaction);
            Object result = session.createReplicaStorageCommand()
                    .executeReplicaRemove(packet.replicationName, packet.mapName, packet.key).get();
            ByteBuffer resultByteBuffer;
            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    session.getTransactionMap(packet.mapName).getValueType().write(writeBuffer, result);
                    resultByteBuffer = writeBuffer.getAndFlipBuffer();
                }
            } else {
                resultByteBuffer = null;
            }
            return new StorageRemoveAck(resultByteBuffer, localTransactionNames);
        }
    }

    private static class PrepareMoveLeafPage implements PacketHandler<StoragePrepareMoveLeafPage> {
        @Override
        public Packet handle(ServerSession session, StoragePrepareMoveLeafPage packet) {
            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            LeafPageMovePlan leafPageMovePlan = map.prepareMoveLeafPage(packet.leafPageMovePlan);
            return new StoragePrepareMoveLeafPageAck(leafPageMovePlan);
        }
    }

    private static class MoveLeafPage implements PacketHandler<StorageMoveLeafPage> {
        @Override
        public Packet handle(ServerSession session, StorageMoveLeafPage packet) {
            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            ConcurrentUtils.submitTask("Add Leaf Page", () -> {
                map.addLeafPage(packet.pageKey, packet.page, packet.addPage);
            });
            return null;
        }
    }

    private static class ReplicatePages implements PacketHandler<StorageReplicatePages> {
        @Override
        public Packet handle(ServerSession session, StorageReplicatePages packet) {
            ConcurrentUtils.submitTask("Replicate Pages", () -> {
                session.replicatePages(packet.dbName, packet.storageName, packet.pages);
            });
            return null;
        }
    }

    private static class ReadPage implements PacketHandler<StorageReadPage> {
        @Override
        public Packet handle(ServerSession session, StorageReadPage packet) {
            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            ByteBuffer page = map.readPage(packet.pageKey);
            return new StorageReadPageAck(page);
        }
    }

    private static class RemoveLeafPage implements PacketHandler<StorageRemoveLeafPage> {
        @Override
        public Packet handle(ServerSession session, StorageRemoveLeafPage packet) {
            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            map.removeLeafPage(packet.pageKey);
            return null;
        }
    }

    private static String getLocalTransactionNames(ServerSession session, boolean isDistributedTransaction) {
        if (isDistributedTransaction) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        session.setStorageReplicationMode(true);

        if (isDistributedTransaction) {
            return session.getTransaction().getLocalTransactionNames();
        } else {
            return null;
        }
    }
}
