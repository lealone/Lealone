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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface NioEventLoop {

    NioEventLoop getDefaultNioEventLoopImpl();

    default Selector getSelector() {
        return getDefaultNioEventLoopImpl().getSelector();
    }

    default void select() throws IOException {
        getDefaultNioEventLoopImpl().select();
    }

    default void select(long timeout) throws IOException {
        getDefaultNioEventLoopImpl().select(timeout);
    }

    default void register(SocketChannel channel, int ops, Object att) throws ClosedChannelException {
        getDefaultNioEventLoopImpl().register(channel, ops, att);
    }

    default void wakeup() {
        getDefaultNioEventLoopImpl().wakeup();
    }

    default void addSocketChannel(SocketChannel channel) {
        getDefaultNioEventLoopImpl().addSocketChannel(channel);
    }

    default void addNioBuffer(SocketChannel channel, NioBuffer nioBuffer) {
        getDefaultNioEventLoopImpl().addNioBuffer(channel, nioBuffer);
    }

    default void tryRegisterWriteOperation(Selector selector) {
        getDefaultNioEventLoopImpl().tryRegisterWriteOperation(selector);
    }

    default void write(SelectionKey key) {
        getDefaultNioEventLoopImpl().write(key);
    }

    default void closeChannel(SocketChannel channel) {
        getDefaultNioEventLoopImpl().closeChannel(channel);
    }
}
