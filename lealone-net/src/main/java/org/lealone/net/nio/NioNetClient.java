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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpConnection;

public class NioNetClient implements org.lealone.net.NetClient {

    private static final Logger logger = LoggerFactory.getLogger(NioNetClient.class);
    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private static final NioNetClient instance = new NioNetClient();

    public static NioNetClient getInstance() {
        return instance;
    }

    private Selector selector;

    private NioNetClient() {
    }

    private synchronized void openSelector() throws IOException {
        if (selector == null) {
            this.selector = Selector.open();
            ConcurrentUtils.submitTask("Nio-Client-Event-Loop", () -> {
                NioNetClient.this.run();
            });
        }
    }

    private void run() {
        final Selector selector = this.selector;
        while (this.selector != null) {
            try {
                selector.select(200L);
                if (this.selector == null)
                    break;
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (!key.isValid())
                            continue;
                        int readyOps = key.readyOps();
                        if ((readyOps & SelectionKey.OP_READ) != 0) {
                            read(key);
                        } else if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                            Object att = key.attachment();
                            connectionEstablished(key, att);
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                logger.warn(Thread.currentThread().getName() + " run exception", e);
                this.selector = null;
                break;
            }
        }
    }

    private void read(SelectionKey key) {
        AsyncConnection conn = (AsyncConnection) key.attachment();
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            while (true) {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                int count = channel.read(buffer);
                if (count <= 0)
                    break;
                buffer.flip();
                NioBuffer nioBuffer = new NioBuffer(buffer);
                conn.handle(nioBuffer);
            }
        } catch (IOException e) {
            closeChannel(channel);
        }
    }

    private void connectionEstablished(SelectionKey key, Object att) {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.isConnectionPending())
            return;

        Attachment attachment = (Attachment) att;
        AsyncConnection conn;
        try {
            channel.finishConnect();
            NioWritableChannel writableChannel = new NioWritableChannel(selector, channel);
            if (attachment.connectionManager != null) {
                conn = attachment.connectionManager.createConnection(writableChannel, false);
            } else {
                conn = new TcpConnection(writableChannel, this);
            }
            conn.setInetSocketAddress(attachment.inetSocketAddress);
            asyncConnections.put(attachment.inetSocketAddress, conn);
            // channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, conn);
            channel.register(selector, SelectionKey.OP_READ, conn);
            attachment.latch.countDown();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private static class Attachment {
        AsyncConnectionManager connectionManager;
        InetSocketAddress inetSocketAddress;
        CountDownLatch latch;
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint) {
        return createConnection(config, endpoint, null);
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint,
            AsyncConnectionManager connectionManager) {
        if (selector == null) {
            try {
                openSelector();
            } catch (IOException e) {
                throw new RuntimeException("Failed to open selector", e);
            }
        }
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        AsyncConnection conn = asyncConnections.get(inetSocketAddress);
        if (conn == null) {
            synchronized (NioNetClient.class) {
                conn = asyncConnections.get(inetSocketAddress);
                if (conn == null) {
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

                        CountDownLatch latch = new CountDownLatch(1);
                        Attachment attachment = new Attachment();
                        attachment.connectionManager = connectionManager;
                        attachment.inetSocketAddress = inetSocketAddress;
                        attachment.latch = latch;

                        channel.register(selector, SelectionKey.OP_CONNECT, attachment);
                        channel.connect(inetSocketAddress);
                        latch.await();
                        conn = asyncConnections.get(inetSocketAddress);
                    } catch (Exception e) {
                        closeChannel(channel);
                        throw new RuntimeException("Failed to connect " + inetSocketAddress, e);
                    }
                }
            }
        }
        return conn;
    }

    private void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
            removeConnection(inetSocketAddress, false);
        } catch (IOException e1) {
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress, boolean closeClient) {
        asyncConnections.remove(inetSocketAddress);
        if (closeClient && asyncConnections.isEmpty()) {
            try {
                Selector selector = this.selector;
                this.selector = null;
                selector.wakeup();
                selector.close();
            } catch (IOException e) {
            }
        }
    }
}
