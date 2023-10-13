/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.net.NetNode;
import org.lealone.net.WritableChannel;

public class TcpServer extends AsyncServer<TcpServerConnection> {

    @Override
    public String getType() {
        return TcpServerEngine.NAME;
    }

    @Override
    protected int getDefaultPort() {
        return Constants.DEFAULT_TCP_PORT;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        NetNode.setLocalTcpNode(getHost(), getPort());
    }

    @Override
    protected TcpServerConnection createConnection(WritableChannel writableChannel,
            Scheduler scheduler) {
        return new TcpServerConnection(this, writableChannel, scheduler);
    }
}
