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
import java.util.LinkedList;
import java.util.List;

import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetBuffer.WritableBuffer;
import com.lealone.net.NetEventLoop;
import com.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final String localHost;
    private final int localPort;

    private final LinkedList<WritableBuffer> buffers = new LinkedList<>();
    private final NetEventLoop eventLoop;
    private final SocketChannel channel;
    private SelectionKey selectionKey; // 注册成功了才设置

    public NioWritableChannel(Scheduler scheduler, SocketChannel channel) throws IOException {
        this.eventLoop = (NetEventLoop) scheduler.getNetEventLoop();
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
    public List<WritableBuffer> getBuffers() {
        return buffers;
    }

    @Override
    public void addBuffer(WritableBuffer buffer) {
        buffers.add(buffer);
    }

    @Override
    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public NetEventLoop getEventLoop() {
        return eventLoop;
    }

    @Override
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    @Override
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    @Override
    public boolean isClosed() {
        return selectionKey == null;
    }

    @Override
    public void close() {
        eventLoop.closeChannel(this);
        selectionKey = null;
        for (WritableBuffer buffer : buffers)
            buffer.recycle();
        buffers.clear();
    }

    @Override
    public void read() {
        throw new UnsupportedOperationException("read");
    }

    @Override
    public void write(WritableBuffer buffer) {
        eventLoop.write(this, buffer);
    }
}
