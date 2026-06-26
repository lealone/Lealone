/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http;

import java.io.IOException;

import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetEventLoop;
import com.lealone.net.TransferConnection;
import com.lealone.net.WritableChannel;
import com.lealone.server.http.util.net.NioEndpoint;
import com.lealone.server.http.util.net.NioSocketWrapper;
import com.lealone.server.http.util.net.SocketEvent;

public class HttpServerConnection extends TransferConnection {

    private final HttpServer httpServer;
    private final NioSocketWrapper socketWrapper;

    public HttpServerConnection(HttpServer httpServer, WritableChannel channel, Scheduler scheduler) {
        super(channel, true);
        this.httpServer = httpServer;
        NioEndpoint endpoint = (NioEndpoint) (httpServer.getProtocolHandler().getEndpoint());
        socketWrapper = endpoint.createSocketWrapper(writableChannel.getSocketChannel());
        NetEventLoop netEventLoop = (NetEventLoop) scheduler.getNetEventLoop();
        socketWrapper.setSelector(netEventLoop.getSelector());
        socketWrapper.setSchedulerId(scheduler.getId());
        // scheduler.getInputBuffer().getDataBuffer().growCapacity(16 * 1024);
    }

    @Override
    public int getPacketLengthByteCount() {
        return -1;
    }

    // private boolean http2;

    // @Override
    // public int getPacketLengthByteCount() {
    // return http2 ? 9 : -1;
    // }
    //
    // @Override
    // public int getPacketLength(ByteBuffer buffer) {
    // if (socketWrapper.getFrameHandler() == null)
    // socketWrapper.getFrameHandler();
    // return socketWrapper.getFrameHandler().handleHeader(buffer);
    // }

    @Override
    public void handle(NetBuffer buffer, boolean autoRecycle) {
        // long t1 = System.nanoTime();
        handle0(buffer);
        if (socketWrapper.isClosed())
            httpServer.removeConnection(this);
        // else
        // http2 = true;
        socketWrapper.setContinueParsing(false);
        try {
            socketWrapper.getSocket().batchWrite();
        } catch (IOException e) {
            socketWrapper.close();
            httpServer.removeConnection(this);
        }
        // System.out.println("time: " + (System.nanoTime() - t1) / 1000);
    }

    private void handle0(NetBuffer buffer) {
        // if (http2) {
        // if (socketWrapper.getFrameHandler() != null) {
        // socketWrapper.getFrameHandler().handleBody(buffer.getByteBuffer());
        // return;
        // }
        if (socketWrapper.processPendingOperation())
            return;
        socketWrapper.processSocket(SocketEvent.OPEN_READ, false);
    }
}
