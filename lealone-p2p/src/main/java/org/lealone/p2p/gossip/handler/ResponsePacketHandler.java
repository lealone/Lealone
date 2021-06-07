/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import java.util.concurrent.TimeUnit;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.p2p.gossip.protocol.GossipResponse;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;
import org.lealone.p2p.server.CallbackInfo;
import org.lealone.p2p.server.IAsyncCallback;
import org.lealone.p2p.server.IAsyncCallbackWithFailure;
import org.lealone.p2p.server.MessagingService;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ResponsePacketHandler implements P2pPacketHandler<GossipResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ResponsePacketHandler.class);

    @Override
    public void handle(P2pPacketIn<GossipResponse> packetIn, int id) {
        long latency = TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - MessagingService.instance().getRegisteredCallbackAge(id));
        CallbackInfo callbackInfo = MessagingService.instance().removeRegisteredCallback(id);
        if (callbackInfo == null) {
            String msg = "Callback already removed for {} (from {})";
            if (logger.isDebugEnabled())
                logger.debug(msg, id, packetIn.from);
            return;
        }

        IAsyncCallback cb = callbackInfo.callback;
        if (packetIn.isFailureResponse()) {
            ((IAsyncCallbackWithFailure) cb).onFailure(packetIn.from);
        } else {
            // TODO: Should we add latency only in success cases?
            MessagingService.instance().maybeAddLatency(cb, packetIn.from, latency);
            cb.response(packetIn);
        }
    }
}
