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

import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.ServerSession;
import org.lealone.server.TcpServerConnection;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PrepareReadParams;
import org.lealone.server.protocol.PrepareReadParamsAck;
import org.lealone.sql.PreparedSQLStatement;

public class PrepareReadParamsHandler implements PacketHandler<PrepareReadParams> {
    @Override
    public Packet handle(TcpServerConnection conn, ServerSession session, PrepareReadParams packet) {
        PreparedSQLStatement command = session.prepareStatement(packet.sql, -1);
        command.setId(packet.commandId);
        conn.addCache(packet.commandId, command);
        boolean isQuery = command.isQuery();
        List<? extends CommandParameter> params = command.getParameters();
        return new PrepareReadParamsAck(isQuery, params);
    }
}
