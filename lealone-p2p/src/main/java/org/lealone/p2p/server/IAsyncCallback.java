/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import org.lealone.p2p.gossip.protocol.P2pPacket;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;

/**
 * implementors of IAsyncCallback need to make sure that any public methods
 * are threadsafe with respect to response() being called from the message
 * service.  In particular, if any shared state is referenced, making
 * response alone synchronized will not suffice.
 */
public interface IAsyncCallback<T extends P2pPacket> {
    /**
     * @param msg response received.
     */
    void response(P2pPacketIn<T> msg);

    /**
     * @return true if this callback is on the read path and its latency should be
     * given as input to the dynamic snitch.
     */
    boolean isLatencyForSnitch();
}
