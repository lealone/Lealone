/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

public class TcpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "TCP";
    private final TcpServer tcpServer = new TcpServer();

    public TcpServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return tcpServer;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        tcpServer.init(config);
    }

    @Override
    public void close() {
        tcpServer.stop();
    }
}
