/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import org.apache.tomcat.util.net.NioEndpoint.NioSocketWrapper;
import org.apache.tomcat.util.net.SocketEvent;

import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetBuffer;
import com.lealone.net.TransferConnection;
import com.lealone.net.WritableChannel;

@SuppressWarnings("unused")
public class TomcatServerConnection extends TransferConnection {

    private final TomcatServer httpServer;
    private final TomcatNioEndpoint endpoint;
    private final NioSocketWrapper socketWrapper;

    public TomcatServerConnection(TomcatServer httpServer, WritableChannel channel,
            Scheduler scheduler) {
        super(channel, true);
        this.httpServer = httpServer;
        endpoint = (TomcatNioEndpoint) (httpServer.getProtocolHandler().getEndpoint());
        socketWrapper = endpoint.createSocketWrapper(writableChannel.getSocketChannel());
    }

    @Override
    public int getPacketLengthByteCount() {
        return -1;
    }

    @Override
    public void handle(NetBuffer buffer, boolean autoRecycle) {
        // httpServer.submit(socketProcessor);
        endpoint.processSocket(socketWrapper, SocketEvent.OPEN_READ, false);
    }
}
