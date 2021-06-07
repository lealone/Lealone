/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.protocol;

import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;

public interface P2pPacket extends Packet {
    @Override
    default PacketType getAckType() {
        return PacketType.VOID;
    }
}
