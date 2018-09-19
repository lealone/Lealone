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
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetBufferFactory;
import org.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private static final Logger logger = LoggerFactory.getLogger(NioWritableChannel.class);
    private final Selector selector;
    private final SocketChannel channel;
    private final InetSocketAddress address;

    public NioWritableChannel(Selector selector, SocketChannel channel) throws IOException {
        this.selector = selector;
        this.channel = channel;
        SocketAddress sa = channel.getRemoteAddress();
        if (sa instanceof InetSocketAddress) {
            address = (InetSocketAddress) sa;
        } else {
            address = null;
        }
    }

    @Override
    public void write(Object data) {
        if (data instanceof NioBuffer) {
            ByteBuffer buffer = ((NioBuffer) data).getBuffer();
            buffer.flip();
            try {
                channel.write(buffer);
                selector.wakeup();
            } catch (IOException e) {
                logger.error("write exception", e);
            }
        }
    }

    @Override
    public void close() {
        try {
            channel.register(selector, 0);
            channel.close();
        } catch (IOException e) {
            logger.error("close exception", e);
        }
    }

    @Override
    public String getHost() {
        return address == null ? "" : address.getHostString();
    }

    @Override
    public int getPort() {
        return address == null ? -1 : address.getPort();
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NioBufferFactory.getInstance();
    }

}
