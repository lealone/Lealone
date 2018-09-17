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

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyNetServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NettyNetServerHandler.class);

    private final NettyNetServer server;
    private final AsyncConnection conn;

    public NettyNetServerHandler(NettyNetServer server, AsyncConnection conn) {
        this.server = server;
        this.conn = conn;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        String msg = "RemoteAddress " + ctx.channel().remoteAddress() + " Unregistered";
        logger.info(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String msg = "RemoteAddress " + ctx.channel().remoteAddress() + " Inactive";
        logger.info(msg);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buff = (ByteBuf) msg;
            conn.handle(new NettyBuffer(buff));
            buff.release();
        } else {
            throw DbException.throwInternalError("msg type is " + msg.getClass().getName() + " not ByteBuf");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
        String msg = "RemoteAddress " + ctx.channel().remoteAddress();
        logger.error(msg + " exception: " + cause.getMessage(), cause);
        logger.info(msg + " closed");
        server.removeConnection(conn);
    }
}
