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

import org.lealone.common.util.MapUtils;

public interface NetEventLoop {

    Object getOwner();

    void setOwner(Object owner);

    Selector getSelector();

    void select() throws IOException;

    void select(long timeout) throws IOException;

    void register(AsyncConnection conn);

    void register(SocketChannel channel, int ops, Object att) throws ClosedChannelException;

    void wakeup();

    void addSocketChannel(SocketChannel channel);

    void addNetBuffer(SocketChannel channel, NetBuffer netBuffer);

    void read(SelectionKey key);

    void write();

    void write(SelectionKey key);

    void closeChannel(SocketChannel channel);

    void close();

    public static boolean isRunInScheduler(Map<String, String> config) {
        return MapUtils.getBoolean(config, "net_event_loop_run_in_scheduler", true);
    }

    public static boolean isAccepterRunInScheduler(Map<String, String> config) {
        return MapUtils.getBoolean(config, "accepter_run_in_scheduler", true);
    }
}
