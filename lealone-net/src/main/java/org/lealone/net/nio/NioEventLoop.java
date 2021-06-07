/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.lealone.net.AsyncConnection;

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

    default void read(SelectionKey key, NioEventLoop nioEventLoop) {
        getDefaultNioEventLoopImpl().read(key, nioEventLoop);
    }

    default void write(SelectionKey key) {
        getDefaultNioEventLoopImpl().write(key);
    }

    default void closeChannel(SocketChannel channel) {
        getDefaultNioEventLoopImpl().closeChannel(channel);
    }

    default void handleException(AsyncConnection conn, SocketChannel channel, Exception e) {
        getDefaultNioEventLoopImpl().handleException(conn, channel, e);
    }
}
