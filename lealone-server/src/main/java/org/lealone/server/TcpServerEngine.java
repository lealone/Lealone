/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

public class TcpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "TCP";

    public TcpServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new TcpServer();
    }
}
