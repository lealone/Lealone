/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server.protocol;

public abstract class RequestPacket extends Packet {

    public void setHeader(PacketInput in) {
        packetLength = in.getPacketLength();
        packetId = in.getPacketId();
    }
}
