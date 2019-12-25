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

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.dt.DistributedTransactionAddSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionCommit;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedQuery;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionQuery;
import org.lealone.server.protocol.dt.DistributedTransactionQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionRollback;
import org.lealone.server.protocol.dt.DistributedTransactionRollbackSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionValidate;
import org.lealone.server.protocol.dt.DistributedTransactionValidateAck;

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

    private static class Query extends QueryPacketHandler<DistributedTransactionQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DistributedTransactionQuery packet) {
            Session session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DistributedTransactionQueryAck(result, rowCount, fetch,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class PreparedQuery extends PreparedQueryPacketHandler<DistributedTransactionPreparedQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, DistributedTransactionPreparedQuery packet) {
            final Session session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new DistributedTransactionPreparedQueryAck(result, rowCount, fetch,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class Update extends UpdatePacketHandler<DistributedTransactionUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DistributedTransactionUpdate packet) {
            final Session session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DistributedTransactionUpdateAck(updateCount,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<DistributedTransactionPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, DistributedTransactionPreparedUpdate packet) {
            final Session session = task.session;
            session.setAutoCommit(false);
            session.setRoot(false);
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new DistributedTransactionPreparedUpdateAck(updateCount,
                    task.session.getTransaction().getLocalTransactionNames());
        }
    }

    private static class Commit implements PacketHandler<DistributedTransactionCommit> {
        @Override
        public Packet handle(ServerSession session, DistributedTransactionCommit packet) {
            session.commit(packet.allLocalTransactionNames);
            return null;
        }
    }

    private static class Rollback implements PacketHandler<DistributedTransactionRollback> {
        @Override
        public Packet handle(ServerSession session, DistributedTransactionRollback packet) {
            session.rollback();
            return null;
        }
    }

    private static class AddSavepoint implements PacketHandler<DistributedTransactionAddSavepoint> {
        @Override
        public Packet handle(ServerSession session, DistributedTransactionAddSavepoint packet) {
            session.addSavepoint(packet.name);
            return null;
        }
    }

    private static class RollbackSavepoint implements PacketHandler<DistributedTransactionRollbackSavepoint> {
        @Override
        public Packet handle(ServerSession session, DistributedTransactionRollbackSavepoint packet) {
            session.rollbackToSavepoint(packet.name);
            return null;
        }
    }

    private static class Validate implements PacketHandler<DistributedTransactionValidate> {
        @Override
        public Packet handle(ServerSession session, DistributedTransactionValidate packet) {
            boolean isValid = session.validateTransaction(packet.localTransactionName);
            return new DistributedTransactionValidateAck(isValid);
        }
    }
}
