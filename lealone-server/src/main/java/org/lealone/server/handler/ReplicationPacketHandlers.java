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

import org.lealone.db.ServerSession;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ReplicationCheckConflict;
import org.lealone.server.protocol.ReplicationCheckConflictAck;
import org.lealone.server.protocol.ReplicationCommit;
import org.lealone.server.protocol.ReplicationHandleConflict;
import org.lealone.server.protocol.ReplicationRollback;

public class ReplicationPacketHandlers extends PacketHandlers {

    static {
        register(PacketType.COMMAND_REPLICATION_COMMIT, new Commit());
        register(PacketType.COMMAND_REPLICATION_ROLLBACK, new Rollback());
        register(PacketType.COMMAND_REPLICATION_CHECK_CONFLICT, new CheckConflict());
        register(PacketType.COMMAND_REPLICATION_HANDLE_CONFLICT, new HandleConflict());
    }

    private static class Commit implements PacketHandler<ReplicationCommit> {
        @Override
        public Packet handle(ServerSession session, ReplicationCommit packet) {
            session.replicationCommit(packet.validKey, packet.autoCommit);
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
