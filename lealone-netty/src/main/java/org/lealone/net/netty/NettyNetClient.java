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
package org.lealone.net.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpConnection;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyNetClient implements org.lealone.net.NetClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyNetClient.class);

    // 使用InetSocketAddress为key而不是字符串，是因为像localhost和127.0.0.1这两种不同格式实际都是同一个意思，
    // 如果用字符串，就会产生两条AsyncConnection，这是没必要的。
    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();

    private static final NettyNetClient instance = new NettyNetClient();

    public static NettyNetClient getInstance() {
        return instance;
    }

    private static NettyNetClient.NettyClient client;

    private static synchronized void openClient(NettyNetClient nettyNetClient, Map<String, String> config) {
        if (client == null) {
            client = new NettyClient(nettyNetClient, config);
        }
    }

    private static synchronized void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private NettyNetClient() {
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint) {
        return createConnection(config, endpoint, null);
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint,
            AsyncConnectionManager connectionManager) {
        if (client == null) {
            openClient(this, config);
        }
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        AsyncConnection asyncConnection = asyncConnections.get(inetSocketAddress);
        if (asyncConnection == null) {
            synchronized (NettyNetClient.class) {
                asyncConnection = asyncConnections.get(inetSocketAddress);
                if (asyncConnection == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    try {
                        client.connect(inetSocketAddress, endpoint.getHost(), endpoint.getPort(), latch,
                                connectionManager);
                        latch.await();
                        asyncConnection = asyncConnections.get(inetSocketAddress);
                    } catch (Exception e) {
                        throw new RuntimeException("Cannot connect to " + inetSocketAddress, e);
                    }
                    if (asyncConnection == null) {
                        throw new RuntimeException("Cannot connect to " + inetSocketAddress);
                    }
                }
            }
        }
        return asyncConnection;
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress, boolean closeClient) {
        removeConnectionInternal(inetSocketAddress, closeClient);
    }

    private static void removeConnectionInternal(AsyncConnection conn, boolean closeClient) {
        removeConnectionInternal(conn.getInetSocketAddress(), closeClient);
    }

    private static synchronized void removeConnectionInternal(InetSocketAddress inetSocketAddress,
            boolean closeClient) {
        asyncConnections.remove(inetSocketAddress);
        if (closeClient && asyncConnections.isEmpty()) {
            closeClient();
        }
    }

    private static class NettyClient {
        private final NettyNetClient nettyNetClient;
        private final Bootstrap bootstrap;

        public NettyClient(NettyNetClient nettyNetClient, Map<String, String> config) {
            this.nettyNetClient = nettyNetClient;
            EventLoopGroup group = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 延迟到connect成功后再创建连接
                        }
                    });
        }

        public void close() {
            bootstrap.config().group().shutdownGracefully();
        }

        public void connect(InetSocketAddress inetSocketAddress, String host, int port, CountDownLatch latch,
                AsyncConnectionManager connectionManager) throws Exception {
            // ChannelFuture f = b.connect(host, port).sync();
            bootstrap.connect(host, port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        SocketChannel ch = (SocketChannel) future.channel();
                        NettyWritableChannel channel = new NettyWritableChannel(ch);
                        AsyncConnection conn;
                        if (connectionManager != null) {
                            conn = connectionManager.createConnection(channel, false);
                        } else {
                            conn = new TcpConnection(channel, nettyNetClient);
                        }
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new NettyClientHandler(connectionManager, conn));
                        conn.setInetSocketAddress(inetSocketAddress);
                        asyncConnections.put(inetSocketAddress, conn);
                    }
                    latch.countDown();
                }
            });
            // f.channel().closeFuture().sync();
        }
    }

    private static class NettyClientHandler extends ChannelInboundHandlerAdapter {

        private final AsyncConnectionManager connectionManager;
        private final AsyncConnection conn;

        public NettyClientHandler(AsyncConnectionManager connectionManager, AsyncConnection conn) {
            this.connectionManager = connectionManager;
            this.conn = conn;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buff = (ByteBuf) msg;
                conn.handle(new NettyBuffer(buff));
                buff.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            String msg = "RemoteAddress " + ctx.channel().remoteAddress();
            logger.error(msg + " exception: " + cause.getMessage(), cause);
            logger.info(msg + " closed");
            if (connectionManager != null)
                connectionManager.removeConnection(conn);
            removeConnectionInternal(conn, false);
        }
    }
}