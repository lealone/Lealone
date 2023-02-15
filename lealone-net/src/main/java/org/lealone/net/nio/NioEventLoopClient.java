/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.ThreadUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;

class NioEventLoopClient extends NetClientBase {

    private static final Logger logger = LoggerFactory.getLogger(NioEventLoopClient.class);
    private static final NioEventLoopClient instance = new NioEventLoopClient();

    public static NioEventLoopClient getInstance() {
        return instance;
    }

    private NioEventLoop nioEventLoop;

    private NioEventLoopClient() {
    }

    private synchronized void createNioEventLoop(Map<String, String> config) {
        if (nioEventLoop == null) {
            try {
                nioEventLoop = new NioEventLoop(config, "client_nio_event_loop_interval", 1000); // 默认1秒
                nioEventLoop.setOwner(this);
                ThreadUtils.start("ClientNioEventLoopService", () -> {
                    NioEventLoopClient.this.run();
                });
            } catch (IOException e) {
                throw new RuntimeException("Failed to open NioEventLoopAdapter", e);
            }
        }
    }

    private void run() {
        long lastTime = System.currentTimeMillis();
        for (;;) {
            try {
                nioEventLoop.select();
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastTime > 1000) {
                    lastTime = currentTime;
                    checkTimeout(currentTime);
                }
                if (isClosed())
                    break;
                nioEventLoop.write();
                Set<SelectionKey> keys = nioEventLoop.getSelector().selectedKeys();
                if (!keys.isEmpty()) {
                    try {
                        for (SelectionKey key : keys) {
                            if (key.isValid()) {
                                int readyOps = key.readyOps();
                                if ((readyOps & SelectionKey.OP_READ) != 0) {
                                    nioEventLoop.read(key);
                                } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                    nioEventLoop.write(key);
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
        try {
            AsyncConnection conn;
            channel.finishConnect();
            nioEventLoop.addSocketChannel(channel);
            NioWritableChannel writableChannel = new NioWritableChannel(channel, nioEventLoop);
            if (attachment.connectionManager != null) {
                conn = attachment.connectionManager.createConnection(writableChannel, false);
            } else {
                conn = new TcpClientConnection(writableChannel, this, attachment.maxSharedSize);
            }
            conn.setInetSocketAddress(attachment.inetSocketAddress);
            AsyncConnection conn2 = addConnection(attachment.inetSocketAddress, conn);
            attachment.conn = conn2;
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(conn2);
            }
            if (conn2 == conn)
                channel.register(nioEventLoop.getSelector(), SelectionKey.OP_READ, attachment);
        } catch (Exception e) {
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(e);
            }
        }
    }

    private static class ClientAttachment extends NioAttachment {
        AsyncConnectionManager connectionManager;
        InetSocketAddress inetSocketAddress;
        AsyncCallback<AsyncConnection> ac;
        int maxSharedSize;
    }

    @Override
    protected synchronized void openInternal(Map<String, String> config) {
        if (nioEventLoop == null) {
            createNioEventLoop(config);
        }
    }

    @Override
    protected synchronized void closeInternal() {
        if (nioEventLoop != null) {
            nioEventLoop.close();
            nioEventLoop = null;
        }
    }

    @Override
    protected void createConnectionInternal(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, int maxSharedSize,
            AsyncCallback<AsyncConnection> ac) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        int socketRecvBuffer = MapUtils.getInt(config, "socket_recv_buffer_size", 16 * 1024);
        int socketSendBuffer = MapUtils.getInt(config, "socket_send_buffer_size", 8 * 1024);
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
            attachment.ac = ac;
            attachment.maxSharedSize = maxSharedSize;

            nioEventLoop.register(channel, SelectionKey.OP_CONNECT, attachment);
            channel.connect(inetSocketAddress);
        } catch (Exception e) {
            nioEventLoop.closeChannel(channel);
            ac.setAsyncResult(e);
        }
    }
}
