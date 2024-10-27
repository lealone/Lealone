/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.channels.SocketChannel;

import com.lealone.db.DataBufferFactory;

public interface WritableChannel {

    void write(NetBuffer data);

    void close();

    String getHost();

    int getPort();

    String getLocalHost();

    int getLocalPort();

    default SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException("getSocketChannel");
    }

    NetBufferFactory getBufferFactory();

    default void setEventLoop(NetEventLoop eventLoop) {
    }

    default boolean isBio() {
        return false;
    }

    default void read() {
    }

    default DataBufferFactory getDataBufferFactory() {
        return DataBufferFactory.getConcurrentFactory();
    }
}
