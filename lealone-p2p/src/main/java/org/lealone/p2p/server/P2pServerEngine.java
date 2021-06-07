/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import java.util.Map;

import org.lealone.net.NetNodeManagerHolder;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngineBase;

public class P2pServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "P2P";

    public P2pServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return P2pServer.instance;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        initNetNodeManager();
        P2pServer.instance.init(config);
    }

    @Override
    public void close() {
        P2pServer.instance.stop();
    }

    private static void initNetNodeManager() {
        NetNodeManagerHolder.set(P2pNetNodeManager.getInstance());
    }

}
