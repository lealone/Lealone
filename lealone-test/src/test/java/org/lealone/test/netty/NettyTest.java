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
package org.lealone.test.netty;

import java.nio.channels.spi.SelectorProvider;

import org.lealone.test.netty.echo.EchoServerHandler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyTest {

    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    public static void main(String[] args) throws Exception {

        org.lealone.test.TestBase.optimizeNetty();

        long t0 = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        // 这行执行很慢
        // String[] a = ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites();
        // System.out.println(a.length);
        long t2 = System.currentTimeMillis();
        // System.out.println("getDefaultCipherSuites: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector();
        t2 = System.currentTimeMillis();
        System.out.println("1 openSelector: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector();
        t2 = System.currentTimeMillis();
        System.out.println("2 openSelector: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        t2 = System.currentTimeMillis();
        System.out.println("1 new NioEventLoopGroup: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        t2 = System.currentTimeMillis();
        System.out.println("2 new NioEventLoopGroup: " + (t2 - t1) + "ms");
        System.out.println("1 total: " + (t2 - t0) + "ms");

        try {
            t1 = System.currentTimeMillis();
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 100)
                    .handler(new LoggingHandler(LogLevel.INFO)).childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            // p.addLast(new LoggingHandler(LogLevel.INFO));
                            p.addLast(new EchoServerHandler());
                        }
                    });
            t2 = System.currentTimeMillis();
            System.out.println("init ServerBootstrap: " + (t2 - t1) + "ms");

            t1 = System.currentTimeMillis();
            // Start the server.
            ChannelFuture f = b.bind(PORT).sync();
            t2 = System.currentTimeMillis();
            System.out.println("bind ServerBootstrap: " + (t2 - t1) + "ms");
            System.out.println("2 total: " + (t2 - t0) + "ms");
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
