/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;

public class NioNetClient extends NetClientBase implements NioEventLoop {

    private static final Logger logger = LoggerFactory.getLogger(NioNetClient.class);
    private static final NioNetClient instance = new NioNetClient();

    public static NioNetClient getInstance() {
        return instance;
    }

    private NioEventLoopAdapter nioEventLoopAdapter;

    private NioNetClient() {
    }

    private synchronized void openNioEventLoopAdapter(Map<String, String> config) {
        if (nioEventLoopAdapter == null) {
            try {
                nioEventLoopAdapter = new NioEventLoopAdapter(config, "client_nio_event_loop_interval", 1000); // 默认1秒
                ConcurrentUtils.submitTask("ClientNioEventLoopService", () -> {
                    NioNetClient.this.run();
                });
            } catch (IOException e) {
                throw new RuntimeException("Failed to open NioEventLoopAdapter", e);
            }
        }
    }

    private void run() {
        for (;;) {
            try {
                nioEventLoopAdapter.select();
                if (isClosed())
                    break;
                Set<SelectionKey> keys = nioEventLoopAdapter.getSelector().selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid()) {
                            int readyOps = key.readyOps();
                            if ((readyOps & SelectionKey.OP_READ) != 0) {
                                read(key, this);
                            } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                write(key);
                            } else if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                                Object att = key.attachment();
                                connectionEstablished(key, att);
                            } else {
                                key.cancel();
                            }
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
                if (isClosed())
                    break;
            } catch (Throwable e) {
                logger.warn(Thread.currentThread().getName() + " run exception: " + e.getMessage(), e);
            }
        }
    }

    private void connectionEstablished(SelectionKey key, Object att) throws Exception {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.isConnectionPending())
            return;

        ClientAttachment attachment = (ClientAttachment) att;
        AsyncConnection conn;
        channel.finishConnect();
        nioEventLoopAdapter.addSocketChannel(channel);
        NioWritableChannel writableChannel = new NioWritableChannel(channel, this);
        if (attachment.connectionManager != null) {
            conn = attachment.connectionManager.createConnection(writableChannel, false);
        } else {
            conn = new TcpClientConnection(writableChannel, this);
        }
        conn.setInetSocketAddress(attachment.inetSocketAddress);
        addConnection(attachment.inetSocketAddress, conn);
        attachment.conn = conn;
        if (attachment.asyncHandler != null) {
            AsyncResult<AsyncConnection> ar = new AsyncResult<>();
            ar.setResult(conn);
            attachment.asyncHandler.handle(ar);
        }
        channel.register(nioEventLoopAdapter.getSelector(), SelectionKey.OP_READ, attachment);
    }

    private static class ClientAttachment extends NioNetServer.Attachment {
        AsyncConnectionManager connectionManager;
        InetSocketAddress inetSocketAddress;
        AsyncHandler<AsyncResult<AsyncConnection>> asyncHandler;
    }

    @Override
    protected synchronized void openInternal(Map<String, String> config) {
        if (nioEventLoopAdapter == null) {
            openNioEventLoopAdapter(config);
        }
    }

    @Override
    protected synchronized void closeInternal() {
        if (nioEventLoopAdapter != null) {
            nioEventLoopAdapter.close();
            nioEventLoopAdapter = null;
        }
    }

    @Override
    protected void createConnectionInternal(NetNode node, AsyncConnectionManager connectionManager,
            AsyncHandler<AsyncResult<AsyncConnection>> asyncHandler) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        int socketRecvBuffer = 16 * 1024;
        int socketSendBuffer = 8 * 1024;
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);

            Socket socket = channel.socket();
            socket.setReceiveBufferSize(socketRecvBuffer);
            socket.setSendBufferSize(socketSendBuffer);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            ClientAttachment attachment = new ClientAttachment();
            attachment.connectionManager = connectionManager;
            attachment.inetSocketAddress = inetSocketAddress;
            attachment.asyncHandler = asyncHandler;

            register(channel, SelectionKey.OP_CONNECT, attachment);
            channel.connect(inetSocketAddress);
        } catch (Exception e) {
            closeChannel(channel);
            AsyncResult<AsyncConnection> ar = new AsyncResult<>();
            ar.setCause(e);
            asyncHandler.handle(ar);
        }
    }

    @Override
    public NioEventLoop getDefaultNioEventLoopImpl() {
        return nioEventLoopAdapter;
    }

    @Override
    public void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
            removeConnection(inetSocketAddress);
        } catch (Exception e1) {
        }
        nioEventLoopAdapter.closeChannel(channel);
    }

    @Override
    public void handleException(AsyncConnection conn, SocketChannel channel, Exception e) {
        closeChannel(channel);
    }
}
