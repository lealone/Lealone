/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.server.protocol.replication.ReplicationHandleConflict;
import org.lealone.server.protocol.replication.ReplicationHandleReplicaConflict;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;
import org.lealone.server.protocol.replication.ReplicationUpdate;

class ReplicationPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.REPLICATION_UPDATE, new Update());
        register(PacketType.REPLICATION_PREPARED_UPDATE, new PreparedUpdate());
        register(PacketType.REPLICATION_CHECK_CONFLICT, new CheckConflict());
        register(PacketType.REPLICATION_HANDLE_CONFLICT, new HandleConflict());
        register(PacketType.REPLICATION_HANDLE_REPLICA_CONFLICT, new HandleReplicaConflict());
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

    private static class HandleReplicaConflict implements PacketHandler<ReplicationHandleReplicaConflict> {
        @Override
        public Packet handle(ServerSession session, ReplicationHandleReplicaConflict packet) {
            session.handleReplicaConflict(packet.retryReplicationNames);
            return null;
        }
    }
}
