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
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

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
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
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
            String localTransactionNames;
            if (packet.isDistributedTransaction) {
                localTransactionNames = session.getTransaction().getLocalTransactionNames();
            } else {
                localTransactionNames = null;
            }
            return new StorageGetAck(resultByteBuffer, localTransactionNames);
        }
    }

    private static class Put implements PacketHandler<StoragePut> {
        @Override
        @SuppressWarnings("unchecked")
        public Packet handle(PacketDeliveryTask task, StoragePut packet) {
            ServerSession session = task.session;
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(packet.replicationName);
            TransactionMap<Object, Object> tmap = session.getTransactionMap(packet.mapName);
            if (packet.addIfAbsent) {
                Transaction.Listener localListener = new Transaction.Listener() {
                    @Override
                    public void operationUndo() {
                        ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                        resultByteBuffer.put((byte) 0);
                        resultByteBuffer.flip();
                        sendResponse(task, packet, resultByteBuffer);
                    }

                    @Override
                    public void operationComplete() {
                        ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                        resultByteBuffer.put((byte) 1);
                        resultByteBuffer.flip();
                        sendResponse(task, packet, resultByteBuffer);
                    }
                };
                tmap.addIfAbsent(tmap.getKeyType().read(packet.key), tmap.getValueType().read(packet.value),
                        localListener);
            } else {
                StorageMap<Object, Object> map = tmap;
                if (packet.raw) {
                    map = (StorageMap<Object, Object>) tmap.getRawMap();
                }
                StorageDataType valueType = map.getValueType();
                Object k = map.getKeyType().read(packet.key);
                Object v = valueType.read(packet.value);
                map.put(k, v, ar -> {
                    if (ar.isSucceeded()) {
                        Object result = ar.getResult();
                        ByteBuffer resultByteBuffer;
                        if (result != null) {
                            try (DataBuffer writeBuffer = DataBuffer.create()) {
                                valueType.write(writeBuffer, result);
                                resultByteBuffer = writeBuffer.getAndFlipBuffer();
                            }
                        } else {
                            resultByteBuffer = null;
                        }
                        sendResponse(task, packet, resultByteBuffer);
                    } else {
                        task.conn.sendError(task.session, task.packetId, ar.getCause());
                    }
                });
            }
            return null;
        }

        private void sendResponse(PacketDeliveryTask task, StoragePut packet, ByteBuffer resultByteBuffer) {
            String localTransactionNames;
            if (packet.isDistributedTransaction) {
                localTransactionNames = task.session.getTransaction().getLocalTransactionNames();
            } else {
                localTransactionNames = null;
            }
            task.conn.sendResponse(task, new StoragePutAck(resultByteBuffer, localTransactionNames));
        }
    }

    private static class Append implements PacketHandler<StorageAppend> {
        @Override
        public Packet handle(ServerSession session, StorageAppend packet) {
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(packet.replicationName);

            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            Object v = map.getValueType().read(packet.value);
            Object result = map.append(v);
            String localTransactionNames;
            if (packet.isDistributedTransaction) {
                localTransactionNames = session.getTransaction().getLocalTransactionNames();
            } else {
                localTransactionNames = null;
            }
            return new StorageAppendAck(((ValueLong) result).getLong(), localTransactionNames);
        }
    }

    private static class Replace implements PacketHandler<StorageReplace> {
        @Override
        public Packet handle(ServerSession session, StorageReplace packet) {
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(packet.replicationName);

            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            StorageDataType valueType = map.getValueType();
            Object key = map.getKeyType().read(packet.key);
            Object oldValue = valueType.read(packet.oldValue);
            Object newValue = valueType.read(packet.newValue);
            boolean result = map.replace(key, oldValue, newValue);
            String localTransactionNames;
            if (packet.isDistributedTransaction) {
                localTransactionNames = session.getTransaction().getLocalTransactionNames();
            } else {
                localTransactionNames = null;
            }
            return new StorageReplaceAck(result, localTransactionNames);
        }
    }

    private static class Remove implements PacketHandler<StorageRemove> {
        @Override
        public Packet handle(ServerSession session, StorageRemove packet) {
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(packet.replicationName);

            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            Object key = map.getKeyType().read(packet.key);
            Object result = map.remove(key);
            String localTransactionNames;
            if (packet.isDistributedTransaction) {
                localTransactionNames = session.getTransaction().getLocalTransactionNames();
            } else {
                localTransactionNames = null;
            }
            ByteBuffer resultByteBuffer;
            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    map.getValueType().write(writeBuffer, result);
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
}
