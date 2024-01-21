/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import com.lealone.net.NetBuffer;
import com.lealone.net.NetBufferFactory;
import com.lealone.net.NetEventLoop;
import com.lealone.net.WritableChannel;

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
    public void write(NetBuffer data) {
        eventLoop.addNetBuffer(channel, data);
    }

    @Override
    public void close() {
        if (eventLoop != null)
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
        return NetBufferFactory.getInstance();
    }

    @Override
    public void setEventLoop(NetEventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }
}
