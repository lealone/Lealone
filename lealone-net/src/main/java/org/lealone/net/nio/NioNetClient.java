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

import java.io.EOFException;
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
import org.lealone.common.util.DateTimeUtils;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpClientConnection;

public class NioNetClient extends NioEventLoopAdapter implements org.lealone.net.NetClient {

    private static final Logger logger = LoggerFactory.getLogger(NioNetClient.class);
    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private static final NioNetClient instance = new NioNetClient();

    public static NioNetClient getInstance() {
        return instance;
    }

    private long loopInterval;
    private Selector selector;

    private NioNetClient() {
    }

    private synchronized void openSelector(Map<String, String> config) throws IOException {
        if (selector == null) {
            // 默认1秒;
            loopInterval = DateTimeUtils.getLoopInterval(config, "client_nio_event_loop_interval", 1000);
            selector = Selector.open();
            setSelector(selector);
            ConcurrentUtils.submitTask("Client-Nio-Event-Loop", () -> {
                NioNetClient.this.run();
            });
        }
    }

    private void run() {
        final Selector selector = this.selector;
        while (this.selector != null) {
            try {
                select(loopInterval);
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
                        } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                            write(key);
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

    // 交由上层去解析协议包是否完整收到更好一点，因为这里假设前4个字节是包的长度，网络层不应该关心具体协议格式
    static ByteBuffer handle(ByteBuffer buffer, ByteBuffer lastBuffer, AsyncConnection conn) throws EOFException {
        if (lastBuffer != null) {
            ByteBuffer buffer2 = ByteBuffer.allocate(lastBuffer.limit() + buffer.limit());
            buffer2.put(lastBuffer).put(buffer);
            buffer2.flip();
            buffer = buffer2;
        }
        while (buffer.hasRemaining()) {
            int remaining = buffer.remaining();
            if (remaining > 4) {
                int pos = buffer.position();
                int length = 0;
                int ch1 = buffer.get(pos) & 0xff;
                int ch2 = buffer.get(pos + 1) & 0xff;
                int ch3 = buffer.get(pos + 2) & 0xff;
                int ch4 = buffer.get(pos + 3) & 0xff;
                if ((ch1 | ch2 | ch3 | ch4) < 0)
                    throw new EOFException();
                length = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
                if (remaining >= 4 + length) {
                    byte[] bytes = new byte[length + 4];
                    buffer.get(bytes);
                    ByteBuffer buffer2 = ByteBuffer.wrap(bytes);
                    NioBuffer nioBuffer = new NioBuffer(buffer2);
                    conn.handle(nioBuffer);
                } else {
                    ByteBuffer buffer2 = ByteBuffer.allocate(remaining);
                    buffer2.put(buffer);
                    buffer2.flip();
                    return buffer2;
                }
            } else {
                ByteBuffer buffer2 = ByteBuffer.allocate(remaining);
                buffer2.put(buffer);
                buffer2.flip();
                return buffer2;
            }
        }
        return null;
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
            addSocketChannel(channel);
            NioWritableChannel writableChannel = new NioWritableChannel(channel, this);
            if (attachment.connectionManager != null) {
                conn = attachment.connectionManager.createConnection(writableChannel, false);
            } else {
                conn = new TcpClientConnection(writableChannel, this);
            }
            conn.setInetSocketAddress(attachment.inetSocketAddress);
            asyncConnections.put(attachment.inetSocketAddress, conn);
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
                openSelector(config);
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

                        register(channel, SelectionKey.OP_CONNECT, attachment);
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

    @Override
    public void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
            removeConnection(inetSocketAddress, false);
        } catch (Exception e1) {
        }
        super.closeChannel(channel);
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
