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

import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;

public class StatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.STATEMENT_QUERY, new Query());
        register(PacketType.STATEMENT_UPDATE, new Update());
    }

    private static class Query extends QueryPacketHandler<StatementQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, StatementQuery packet) {
            return handlePacket(task, packet);
        }
    }

    private static class Update extends UpdatePacketHandler<StatementUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, StatementUpdate packet) {
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new StatementUpdateAck(updateCount);
        }
    }
}
