/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import org.lealone.net.NetNode;
import org.lealone.p2p.gossip.protocol.P2pPacket;

public interface IAsyncCallbackWithFailure<T extends P2pPacket> extends IAsyncCallback<T> {
    /**
     * Called when there is an exception on the remote node or timeout happens
     */
    public void onFailure(NetNode from);
}
