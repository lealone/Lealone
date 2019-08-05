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
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import org.lealone.net.NetBufferFactory;
import org.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private final SocketChannel channel;
    private final NioEventLoop nioEventLoop;
    private final String host;
    private final int port;

    public NioWritableChannel(SocketChannel channel, NioEventLoop nioEventLoop) throws IOException {
        this.channel = channel;
        this.nioEventLoop = nioEventLoop;
        SocketAddress sa = channel.getRemoteAddress();
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) sa;
            host = address.getHostString();
            port = address.getPort();
        } else {
            host = "";
            port = -1;
        }
    }

    @Override
    public void write(Object data) {
        if (data instanceof NioBuffer) {
            nioEventLoop.addNioBuffer(channel, (NioBuffer) data);
        }
    }

    @Override
    public void close() {
        nioEventLoop.closeChannel(channel);
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NioBufferFactory.getInstance();
    }

}
