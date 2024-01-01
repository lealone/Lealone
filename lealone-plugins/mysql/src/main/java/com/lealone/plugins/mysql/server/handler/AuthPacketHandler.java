/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.server.handler;

import com.lealone.plugins.mysql.server.MySQLServerConnection;
import com.lealone.plugins.mysql.server.protocol.AuthPacket;
import com.lealone.plugins.mysql.server.protocol.PacketInput;

public class AuthPacketHandler implements PacketHandler {

    private final MySQLServerConnection conn;

    public AuthPacketHandler(MySQLServerConnection conn) {
        this.conn = conn;
    }

    @Override
    public void handle(PacketInput in) {
        AuthPacket authPacket = new AuthPacket();
        authPacket.read(in);
        conn.authenticate(authPacket);
    }
}
