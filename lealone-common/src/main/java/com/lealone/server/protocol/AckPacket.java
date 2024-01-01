/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol;

public interface AckPacket extends Packet {
    @Override
    default PacketType getAckType() {
        return PacketType.VOID;
    }
}
