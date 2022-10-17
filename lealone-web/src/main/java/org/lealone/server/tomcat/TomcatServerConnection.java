/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import java.nio.ByteBuffer;

import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.SocketProcessorBase;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.lealone.net.NetBuffer;
import org.lealone.net.TransferConnection;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler;

public class TomcatServerConnection extends TransferConnection {

    // private final HttpServer httpServer;
    // private final Scheduler scheduler;

    private final SocketProcessorBase<NioChannel> socketProcessor;

    public TomcatServerConnection(TomcatServer httpServer, WritableChannel channel,
            Scheduler scheduler) {
        super(channel, true);
        // this.httpServer = httpServer;
        // this.scheduler = scheduler;
        // LinkedList<NioChannel> nioChannels = httpServer.getNioChannels(scheduler.getHandlerId());
        TomcatNioEndpoint endpoint = (TomcatNioEndpoint) (httpServer.getProtocolHandler().getEndpoint());
        endpoint.setSelector(scheduler.getNetEventLoop().getSelector());
        SocketWrapperBase<NioChannel> socketWrapper = endpoint
                .createSocketWrapper(writableChannel.getSocketChannel(), null);
        // socketWrapper.setRecycledProcessors(httpServer.getRecycledProcessors(scheduler.getHandlerId()));
        // socketWrapper.setNioChannels(nioChannels);
        socketProcessor = endpoint.createSocketProcessor(socketWrapper, SocketEvent.OPEN_READ);
        // socketWrapper.setSocketProcessor(socketProcessor);
    }

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        return null;
    }

    @Override
    public int getPacketLength() {
        return 0;
    }

    @Override
    public void handle(NetBuffer buffer) {
        socketProcessor.run();
    }
}
