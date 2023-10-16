/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.protocol;

public abstract class RequestPacket extends Packet {
    @Override
    public void read(PacketInput in) {
        packetLength = in.readUB3();
        packetId = in.read();
    }
}
