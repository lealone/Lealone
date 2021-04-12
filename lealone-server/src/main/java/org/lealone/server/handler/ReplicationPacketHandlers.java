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

import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.server.protocol.replication.ReplicationCommit;
import org.lealone.server.protocol.replication.ReplicationHandleConflict;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;
import org.lealone.server.protocol.replication.ReplicationRollback;
import org.lealone.server.protocol.replication.ReplicationUpdate;

class ReplicationPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.REPLICATION_UPDATE, new Update());
        register(PacketType.REPLICATION_PREPARED_UPDATE, new PreparedUpdate());
        register(PacketType.REPLICATION_COMMIT, new Commit());
        register(PacketType.REPLICATION_ROLLBACK, new Rollback());
        register(PacketType.REPLICATION_CHECK_CONFLICT, new CheckConflict());
        register(PacketType.REPLICATION_HANDLE_CONFLICT, new HandleConflict());
    }

    private static class Update extends UpdatePacketHandler<ReplicationUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, ReplicationUpdate packet) {
            task.session.setReplicationName(packet.replicationName);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return task.session.createReplicationUpdateAckPacket(updateCount, false);
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<ReplicationPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, ReplicationPreparedUpdate packet) {
            task.session.setReplicationName(packet.replicationName);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return task.session.createReplicationUpdateAckPacket(updateCount, true);
        }
    }

    private static class Commit implements PacketHandler<ReplicationCommit> {
        @Override
        public Packet handle(ServerSession session, ReplicationCommit packet) {
            session.replicationCommit(packet.validKey, packet.autoCommit, packet.retryReplicationNames);
            return null;
        }
    }

    private static class Rollback implements PacketHandler<ReplicationRollback> {
        @Override
        public Packet handle(ServerSession session, ReplicationRollback packet) {
            session.rollback();
            return null;
        }
    }

    private static class CheckConflict implements PacketHandler<ReplicationCheckConflict> {
        @Override
        public Packet handle(ServerSession session, ReplicationCheckConflict packet) {
            String ret = session.checkReplicationConflict(packet);
            return new ReplicationCheckConflictAck(ret);
        }
    }

    private static class HandleConflict implements PacketHandler<ReplicationHandleConflict> {
        @Override
        public Packet handle(ServerSession session, ReplicationHandleConflict packet) {
            session.handleReplicationConflict(packet);
            return null;
        }
    }
}
