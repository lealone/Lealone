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
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.DataBuffer;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpClientConnection;
import org.lealone.net.Transfer;

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
                ShutdownHookUtils.addShutdownHook(this, () -> {
                    close();
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
                                read(key);
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
                logger.warn(Thread.currentThread().getName() + " run exception: " + e.getMessage());
            }
        }
    }

    // 交由上层去解析协议包是否完整收到更好一点，因为这里假设前4个字节是包的长度，网络层不应该关心具体协议格式
    @Deprecated
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
                    NioBuffer nioBuffer = new NioBuffer(DataBuffer.create(buffer2));
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
        Attachment attachment = (Attachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            while (true) {
                DataBuffer dataBuffer = DataBuffer.create(Transfer.BUFFER_SIZE);
                ByteBuffer buffer = dataBuffer.getBuffer();
                int count = channel.read(buffer);
                if (count > 0) {
                    attachment.endOfStreamCount = 0;
                } else {
                    // 客户端非正常关闭时，可能会触发JDK的bug，导致run方法死循环，selector.select不会阻塞
                    // netty框架在下面这个方法的代码中有自己的不同解决方案
                    // io.netty.channel.nio.NioEventLoop.processSelectedKey
                    if (count < 0) {
                        attachment.endOfStreamCount++;
                        if (attachment.endOfStreamCount > 3) {
                            closeChannel(channel);
                        }
                    }
                    break;
                }
                buffer.flip();
                NioBuffer nioBuffer = new NioBuffer(dataBuffer);
                conn.handle(nioBuffer);
            }
        } catch (IOException e) {
            closeChannel(channel);
        }
    }

    private void connectionEstablished(SelectionKey key, Object att) throws Exception {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.isConnectionPending())
            return;

        Attachment attachment = (Attachment) att;
        AsyncConnection conn;
        try {
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
            channel.register(nioEventLoopAdapter.getSelector(), SelectionKey.OP_READ, attachment);
        } finally {
            attachment.latch.countDown();
        }
    }

    private static class Attachment {
        AsyncConnectionManager connectionManager;
        InetSocketAddress inetSocketAddress;
        CountDownLatch latch;
        AsyncConnection conn;
        int endOfStreamCount;
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
    protected void createConnectionInternal(NetEndpoint endpoint, AsyncConnectionManager connectionManager,
            CountDownLatch latch) throws Exception {
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
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

            Attachment attachment = new Attachment();
            attachment.connectionManager = connectionManager;
            attachment.inetSocketAddress = inetSocketAddress;
            attachment.latch = latch;

            register(channel, SelectionKey.OP_CONNECT, attachment);
            channel.connect(inetSocketAddress);
        } catch (Exception e) {
            closeChannel(channel);
            throw e;
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
}
