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
import org.lealone.server.protocol.dt.DTransactionPreparedQuery;
import org.lealone.server.protocol.dt.DTransactionPreparedQueryAck;
import org.lealone.server.protocol.dt.DTransactionPreparedUpdate;
import org.lealone.server.protocol.dt.DTransactionPreparedUpdateAck;
import org.lealone.server.protocol.dt.DTransactionQuery;
import org.lealone.server.protocol.dt.DTransactionQueryAck;
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
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK, new Rollback());
        register(PacketType.DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT, new AddSavepoint());
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT, new RollbackSavepoint());
        register(PacketType.DISTRIBUTED_TRANSACTION_VALIDATE, new Validate());
    }

    private static class Query extends QueryPacketHandler<DTransactionQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionQuery packet) {
            ServerSession session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet, packet.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DTransactionQueryAck(result, rowCount, fetch,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class PreparedQuery extends PreparedQueryPacketHandler<DTransactionPreparedQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionPreparedQuery packet) {
            final ServerSession session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet, packet.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DTransactionPreparedQueryAck(result, rowCount, fetch,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class Update extends UpdatePacketHandler<DTransactionUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionUpdate packet) {
            final ServerSession session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet, packet.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DTransactionUpdateAck(updateCount, task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<DTransactionPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DTransactionPreparedUpdate packet) {
            final ServerSession session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet, packet.pageKeys);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DTransactionPreparedUpdateAck(updateCount,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class Commit implements PacketHandler<DTransactionCommit> {
        @Override
        public Packet handle(ServerSession session, DTransactionCommit packet) {
            session.commit(packet.allLocalTransactionNames);
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
            boolean isValid = session.validateTransaction(packet.localTransactionName);
            return new DTransactionValidateAck(isValid);
        }
    }
}
