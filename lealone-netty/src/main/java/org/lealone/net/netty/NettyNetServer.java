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

import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetServerBase;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

//TODO 1.支持SSL 2.支持配置参数
public class NettyNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(NettyNetServer.class);

    private EventLoopGroup bossGroup;

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        try {
            bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = bossGroup; // 要不要用额外的线程池?
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new NettyNetServerInitializer(this));
            CountDownLatch latch = new CountDownLatch(1);
            // 这一行会一直阻塞，所以不能这样用
            // b.bind(port).sync().channel().closeFuture().sync();
            b.bind(getHost(), getPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    latch.countDown();
                }
            });
            latch.await();
            super.start();
        } catch (Exception e) {
            logger.error("Failed to start netty net server", e);
            throw DbException.convert(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
    }
}
