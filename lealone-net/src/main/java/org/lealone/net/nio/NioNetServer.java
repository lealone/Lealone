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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetServerBase;

//TODO 1.支持SSL 2.支持配置参数
public class NioNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(NioNetServer.class);
    private static final ConcurrentHashMap<SocketChannel, AsyncConnection> connections = new ConcurrentHashMap<>();
    private Selector selector;
    private ServerSocketChannel serverChannel;

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        logger.info("Starting nio net server");
        try {
            this.selector = Selector.open();
            this.serverChannel = ServerSocketChannel.open();
            this.serverChannel.socket().bind(new InetSocketAddress(getHost(), getPort()));
            this.serverChannel.configureBlocking(false);
            this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            super.start();

            ConcurrentUtils.submitTask("Nio-Server-Event-Loop", () -> {
                NioNetServer.this.run();
            });
        } catch (Exception e) {
            logger.error("Failed to start nio net server", e);
            throw DbException.convert(e);
        }
    }

    private void run() {
        final Selector selector = this.selector;
        for (;;) {
            try {
                selector.select(1000L);
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid()) {
                            int readyOps = key.readyOps();
                            if ((readyOps & SelectionKey.OP_READ) != 0) {
                                read(key);
                            } else if ((readyOps & SelectionKey.OP_ACCEPT) != 0) {
                                accept();
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
            } catch (Throwable e) {
                logger.warn(getName(), e);
            }
        }
    }

    private static class Attachment {
        AsyncConnection conn;
        int endOfStreamCount;
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);

            NioWritableChannel writableChannel = new NioWritableChannel(selector, channel);
            AsyncConnection conn = createConnection(writableChannel, true);

            Attachment attachment = new Attachment();
            attachment.conn = conn;
            channel.register(selector, SelectionKey.OP_READ, attachment);
            connections.put(channel, conn);
        } catch (Throwable e) {
            closeChannel(channel);
            logger.warn(getName(), e);
        }
    }

    private void read(SelectionKey key) {
        Attachment attachment = (Attachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            while (true) {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
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
                NioBuffer nioBuffer = new NioBuffer(buffer);
                conn.handle(nioBuffer);
            }
        } catch (IOException e) {
            closeChannel(channel);
        }
    }

    private void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        connections.remove(channel);
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
    public synchronized void stop() {
        if (isStopped())
            return;
        logger.info("Stopping nio net server");
        super.stop();
        if (serverChannel == null) {
            return;
        }
        try {
            serverChannel.close();
        } catch (IOException e) {
        }
    }
}
