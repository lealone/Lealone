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
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpClientConnection;

public class NioNetClient implements org.lealone.net.NetClient {

    private static final Logger logger = LoggerFactory.getLogger(NioNetClient.class);
    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private static final NioNetClient instance = new NioNetClient();

    public static NioNetClient getInstance() {
        return instance;
    }

    private final AtomicBoolean selecting = new AtomicBoolean(false);
    private Selector selector;
    private long loopInterval;

    private NioNetClient() {
    }

    private synchronized void openSelector(Map<String, String> config) throws IOException {
        if (selector == null) {
            // 默认1秒;
            loopInterval = DateTimeUtils.getLoopInterval(config, "client_nio_event_loop_interval", 1000);
            this.selector = Selector.open();
            ConcurrentUtils.submitTask("Client-Nio-Event-Loop", () -> {
                NioNetClient.this.run();
            });
        }
    }

    private void run() {
        final Selector selector = this.selector;
        while (this.selector != null) {
            try {
                if (selecting.compareAndSet(false, true)) {
                    selector.select(loopInterval);
                    selecting.set(false);
                }
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
                conn = new TcpClientConnection(writableChannel, this);
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

                        // 当nio-event-loop线程执行selector.select被阻塞时，代码内部依然会占用publicKeys锁，
                        // 而另一个线程执行channel.register时，内部也会去要publicKeys锁，从而导致也被阻塞，
                        // 所以下面这段代码的用处是:
                        // 只要发现nio-event-loop线程正在进行select，那么就唤醒它，并释放publicKeys锁。
                        while (true) {
                            if (selecting.compareAndSet(false, true)) {
                                channel.register(selector, SelectionKey.OP_CONNECT, attachment);
                                selecting.set(false);
                                selector.wakeup();
                                break;
                            } else {
                                selector.wakeup();
                            }
                        }
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
