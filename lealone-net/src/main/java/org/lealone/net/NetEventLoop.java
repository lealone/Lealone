/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;

public interface NetEventLoop {

    NetEventLoop getDefaultNetEventLoopImpl();

    default Selector getSelector() {
        return getDefaultNetEventLoopImpl().getSelector();
    }

    default void select() throws IOException {
        getDefaultNetEventLoopImpl().select();
    }

    default void select(long timeout) throws IOException {
        getDefaultNetEventLoopImpl().select(timeout);
    }

    default void register(AsyncConnection conn) {
        getDefaultNetEventLoopImpl().register(conn);
    }

    default void register(SocketChannel channel, int ops, Object att) throws ClosedChannelException {
        getDefaultNetEventLoopImpl().register(channel, ops, att);
    }

    default void wakeup() {
        getDefaultNetEventLoopImpl().wakeup();
    }

    default void addSocketChannel(SocketChannel channel) {
        getDefaultNetEventLoopImpl().addSocketChannel(channel);
    }

    default void addNetBuffer(SocketChannel channel, NetBuffer netBuffer) {
        getDefaultNetEventLoopImpl().addNetBuffer(channel, netBuffer);
    }

    default void read(SelectionKey key, NetEventLoop netEventLoop) {
        getDefaultNetEventLoopImpl().read(key, netEventLoop);
    }

    default void write() {
        getDefaultNetEventLoopImpl().write();
    }

    default void write(SelectionKey key) {
        getDefaultNetEventLoopImpl().write(key);
    }

    default void closeChannel(SocketChannel channel) {
        getDefaultNetEventLoopImpl().closeChannel(channel);
    }

    default void close() {
        getDefaultNetEventLoopImpl().close();
    }

    default void handleException(AsyncConnection conn, SocketChannel channel, Exception e) {
        getDefaultNetEventLoopImpl().handleException(conn, channel, e);
    }

    // 是否每次循环只处理一个包
    default boolean onePacketPerLoop() {
        return false;
    }

    public static boolean isRunInScheduler(Map<String, String> config) {
        String inScheduler = config.get("net_event_loop_run_in_scheduler");
        return inScheduler == null || Boolean.parseBoolean(inScheduler);
    }
}
