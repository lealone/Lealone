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
import org.lealone.server.protocol.storage.StorageRemoveLeafPage;
import org.lealone.server.protocol.storage.StorageReplicateRootPages;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

class StoragePacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.STORAGE_PUT, new Put());
        register(PacketType.STORAGE_APPEND, new Append());
        register(PacketType.STORAGE_GET, new Get());
        register(PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE, new PrepareMoveLeafPage());
        register(PacketType.STORAGE_MOVE_LEAF_PAGE, new MoveLeafPage());
        register(PacketType.STORAGE_REPLICATE_ROOT_PAGES, new ReplicateRootPages());
        register(PacketType.STORAGE_READ_PAGE, new ReadPage());
        register(PacketType.STORAGE_REMOVE_LEAF_PAGE, new RemoveLeafPage());
    }

    private static class Put implements PacketHandler<StoragePut> {
        @Override
        public Packet handle(ServerSession session, StoragePut packet) {
            if (packet.isDistributedTransaction) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(packet.replicationName);

            StorageMap<Object, Object> map = session.getStorageMap(packet.mapName);
            if (packet.raw) {
                map = map.getRawMap();
            }

            StorageDataType valueType = map.getValueType();
            Object k = map.getKeyType().read(packet.key);
            Object v = valueType.read(packet.value);
            Object result = map.put(k, v);

            ByteBuffer resultByteBuffer;
            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    valueType.write(writeBuffer, result);
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
            return new StoragePutAck(resultByteBuffer, localTransactionNames);
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

    private static class ReplicateRootPages implements PacketHandler<StorageReplicateRootPages> {
        @Override
        public Packet handle(ServerSession session, StorageReplicateRootPages packet) {
            ConcurrentUtils.submitTask("Replicate Root Pages", () -> {
                session.replicateRootPages(packet.dbName, packet.rootPages);
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
