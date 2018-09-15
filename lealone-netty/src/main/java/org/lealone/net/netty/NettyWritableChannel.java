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

import org.lealone.net.NetBufferFactory;
import org.lealone.net.WritableChannel;

import io.netty.channel.socket.SocketChannel;

public class NettyWritableChannel implements WritableChannel {

    private final SocketChannel channel;

    public NettyWritableChannel(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void write(Object data) {
        if (data instanceof NettyBuffer) {
            channel.writeAndFlush(((NettyBuffer) data).getBuffer());
        } else {
            channel.writeAndFlush(data);
        }
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public String getHost() {
        return channel.remoteAddress().getHostString();
    }

    @Override
    public int getPort() {
        return channel.remoteAddress().getPort();
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NettyBufferFactory.getInstance();
    }
}
