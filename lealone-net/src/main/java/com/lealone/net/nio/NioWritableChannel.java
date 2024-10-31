/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.lealone.db.DataBufferFactory;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetEventLoop;
import com.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final String localHost;
    private final int localPort;

    private DataBufferFactory dataBufferFactory;
    private final SocketChannel channel;
    private NetEventLoop eventLoop;
    private SelectionKey selectionKey;
    private NetBuffer buffer;

    public NioWritableChannel(DataBufferFactory dataBufferFactory, SocketChannel channel,
            NetEventLoop eventLoop) throws IOException {
        this.dataBufferFactory = dataBufferFactory;
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
        sa = channel.getLocalAddress();
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) sa;
            localHost = address.getHostString();
            localPort = address.getPort();
        } else {
            localHost = "";
            localPort = -1;
        }
        setEventLoop(eventLoop);
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
    public String getLocalHost() {
        return localHost;
    }

    @Override
    public int getLocalPort() {
        return localPort;
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return dataBufferFactory;
    }

    @Override
    public NetBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void setBuffer(NetBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public void setEventLoop(NetEventLoop eventLoop) {
        if (eventLoop != null) {
            selectionKey = channel.keyFor(eventLoop.getSelector());
            this.eventLoop = eventLoop;
        }
    }

    @Override
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    @Override
    public boolean isClosed() {
        return eventLoop == null;
    }

    @Override
    public void close() {
        buffer = null;
        if (eventLoop != null)
            eventLoop.closeChannel(this);
        eventLoop = null;
    }

    @Override
    public void read() {
        throw new UnsupportedOperationException("read");
    }

    @Override
    public void write(NetBuffer buffer) {
        eventLoop.write(this, buffer);
    }
}
