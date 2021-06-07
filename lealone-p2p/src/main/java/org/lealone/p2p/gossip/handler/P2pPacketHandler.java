/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import org.lealone.p2p.gossip.protocol.P2pPacket;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;

/**
 * P2pPacketHandler provides the method that all packet handlers need to implement.
 * The concrete implementation of this interface would provide the functionality
 * for a given packet.
 */
public interface P2pPacketHandler<T extends P2pPacket> {
    /**
     * This method delivers a packet to the implementing class (if the implementing
     * class was registered by a call to P2pPacketHandlers.register).
     * Note that the caller should not be holding any locks when calling this method
     * because the implementation may be synchronized.
     *
     * @param packetIn - incoming packet that needs handling.
     * @param id
     */
    public void handle(P2pPacketIn<T> packetIn, int id);
}
