/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.dt.DTransactionAddSavepoint;
import org.lealone.server.protocol.dt.DTransactionCommit;
import org.lealone.server.protocol.dt.DTransactionCommitAck;
import org.lealone.server.protocol.dt.DTransactionCommitFinal;
import org.lealone.server.protocol.dt.DTransactionParameters;
import org.lealone.server.protocol.dt.DTransactionPreparedQuery;
import org.lealone.server.protocol.dt.DTransactionPreparedQueryAck;
import org.lealone.server.protocol.dt.DTransactionPreparedUpdate;
import org.lealone.server.protocol.dt.DTransactionPreparedUpdateAck;
import org.lealone.server.protocol.dt.DTransactionQuery;
import org.lealone.server.protocol.dt.DTransactionQueryAck;
import org.lealone.server.protocol.dt.DTransactionReplicationPreparedUpdate;
import org.lealone.server.protocol.dt.DTransactionReplicationUpdate;
import org.lealone.server.protocol.dt.DTransactionRollback;
import org.lealone.server.protocol.dt.DTransactionRollbackSavepoint;
import org.lealone.server.protocol.dt.DTransactionUpdate;
import org.lealone.server.protocol.dt.DTransactionUpdateAck;
import org.lealone.server.protocol.dt.DTransactionValidate;
import org.lealone.server.protocol.dt.DTransactionValidateAck;

class DistributedTransactionPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.DISTRIBUTED_TRANSACTION_QUERY, new Query());
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY, new PreparedQuery());
        register(PacketType.DISTRIBUTED_TRANSACTION_UPDATE, new Update());
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE, new PreparedUpdate());
        register(PacketType.DISTRIBUTED_TRANSACTION_COMMIT, new Commit());
        register(PacketType.DISTRIBUTED_TRANSACTION_COMMIT_FINAL, new CommitFinal());
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK, new Rollback());
        register(PacketType.DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT, new AddSavepoint());
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT, new RollbackSavepoint());
        register(PacketType.DISTRIBUTED_TRANSACTION_VALIDATE, new Validate());
        register(PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_UPDATE, new ReplicationUpdate());
        register(PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE, new ReplicationPreparedUpdate());
    }

    private static class Query extends QueryPacketHandler<DTransactionQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionQuery packet) {
            initSession(task, packet.parameters);
            return handlePacket(task, packet, packet.parameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DTransactionQueryAck(result, rowCount, fetch);
        }
    }

    private static class PreparedQuery extends PreparedQueryPacketHandler<DTransactionPreparedQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionPreparedQuery packet) {
            initSession(task, packet.dtParameters);
            return handlePacket(task, packet, packet.dtParameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DTransactionPreparedQueryAck(result, rowCount, fetch);
        }
    }

    private static class Update extends UpdatePacketHandler<DTransactionUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionUpdate packet) {
            initSession(task, packet.parameters);
            return handlePacket(task, packet, packet.parameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DTransactionUpdateAck(updateCount);
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<DTransactionPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionPreparedUpdate packet) {
            initSession(task, packet.dtParameters);
            return handlePacket(task, packet, packet.dtParameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DTransactionPreparedUpdateAck(updateCount);
        }
    }

    private static class Commit implements PacketHandler<DTransactionCommit> {
        @Override
        public Packet handle(ServerSession session, DTransactionCommit packet) {
            session.commit(packet.globalTransactionName);
            return new DTransactionCommitAck();
        }
    }

    private static class CommitFinal implements PacketHandler<DTransactionCommitFinal> {
        @Override
        public Packet handle(ServerSession session, DTransactionCommitFinal packet) {
            session.commitFinal();
            return null;
        }
    }

    private static class Rollback implements PacketHandler<DTransactionRollback> {
        @Override
        public Packet handle(ServerSession session, DTransactionRollback packet) {
            session.rollback();
            return null;
        }
    }

    private static class AddSavepoint implements PacketHandler<DTransactionAddSavepoint> {
        @Override
        public Packet handle(ServerSession session, DTransactionAddSavepoint packet) {
            session.addSavepoint(packet.name);
            return null;
        }
    }

    private static class RollbackSavepoint implements PacketHandler<DTransactionRollbackSavepoint> {
        @Override
        public Packet handle(ServerSession session, DTransactionRollbackSavepoint packet) {
            session.rollbackToSavepoint(packet.name);
            return null;
        }
    }

    private static class Validate implements PacketHandler<DTransactionValidate> {
        @Override
        public Packet handle(ServerSession session, DTransactionValidate packet) {
            boolean isValid = session.validateTransaction(packet.globalTransactionName);
            return new DTransactionValidateAck(isValid);
        }
    }

    private static class ReplicationUpdate extends UpdatePacketHandler<DTransactionReplicationUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionReplicationUpdate packet) {
            initSession(task, packet.parameters, packet.replicationName);
            return handlePacket(task, packet, packet.parameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return task.session.createReplicationUpdateAckPacket(updateCount, false);
        }
    }

    private static class ReplicationPreparedUpdate
            extends PreparedUpdatePacketHandler<DTransactionReplicationPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionReplicationPreparedUpdate packet) {
            initSession(task, packet.parameters, packet.replicationName);
            return handlePacket(task, packet, packet.parameters.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return task.session.createReplicationUpdateAckPacket(updateCount, true);
        }
    }

    private static void initSession(PacketDeliveryTask task, DTransactionParameters parameters) {
        initSession(task, parameters, null);
    }

    private static void initSession(PacketDeliveryTask task, DTransactionParameters parameters,
            String replicationName) {
        ServerSession session = task.session;
        session.setRoot(false);
        if (parameters != null)
            session.setAutoCommit(parameters.autoCommit);
        if (replicationName != null)
            session.setReplicationName(replicationName);
    }
}
