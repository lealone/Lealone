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

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncCallback;
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
        long lastTime = System.currentTimeMillis();
        for (;;) {
            try {
                nioEventLoopAdapter.select();
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastTime > 1000) {
                    lastTime = currentTime;
                    checkTimeout(currentTime);
                }
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
        try {
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
            AsyncConnection conn2 = addConnection(attachment.inetSocketAddress, conn);
            attachment.conn = conn2;
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(conn2);
            }
            if (conn2 == conn)
                channel.register(nioEventLoopAdapter.getSelector(), SelectionKey.OP_READ, attachment);
        } catch (Exception e) {
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(e);
            }
        }
    }

    private static class ClientAttachment extends NioNetServer.Attachment {
        AsyncConnectionManager connectionManager;
        InetSocketAddress inetSocketAddress;
        AsyncCallback<AsyncConnection> ac;
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
            AsyncCallback<AsyncConnection> ac) {
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
            attachment.ac = ac;

            register(channel, SelectionKey.OP_CONNECT, attachment);
            channel.connect(inetSocketAddress);
        } catch (Exception e) {
            closeChannel(channel);
            ac.setAsyncResult(e);
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
        nioEventLoopAdapter.closeChannel(channel);
    }

    @Override
    public void handleException(AsyncConnection conn, SocketChannel channel, Exception e) {
        conn.handleException(e);
        try {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
            removeConnection(inetSocketAddress);
        } catch (Exception e1) {
        }
        closeChannel(channel);
    }
}
