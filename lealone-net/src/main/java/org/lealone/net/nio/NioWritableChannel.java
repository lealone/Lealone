/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import org.lealone.net.NetBufferFactory;
import org.lealone.net.NetEventLoop;
import org.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private final SocketChannel channel;
    private final String host;
    private final int port;
    private NetEventLoop eventLoop;

    public NioWritableChannel(SocketChannel channel, NetEventLoop eventLoop) throws IOException {
        this.channel = channel;
        SocketAddress sa = channel.getRemoteAddress();
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) sa;
            host = address.getHostString();
            port = address.getPort();
        } else {
            host = "";
            port = -1;
        }
        this.eventLoop = eventLoop;
    }

    @Override
    public void write(Object data) {
        if (data instanceof NioBuffer) {
            eventLoop.addNetBuffer(channel, (NioBuffer) data);
        }
    }

    @Override
    public void close() {
        eventLoop.closeChannel(channel);
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NioBufferFactory.getInstance();
    }

    @Override
    public void setEventLoop(NetEventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }
}
