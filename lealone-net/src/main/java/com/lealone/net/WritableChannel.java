/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.channels.SocketChannel;

import com.lealone.db.DataBufferFactory;

public interface WritableChannel {

    String getHost();

    int getPort();

    String getLocalHost();

    int getLocalPort();

    default boolean isBio() {
        return false;
    }

    default SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException("getSocketChannel");
    }

    default void setEventLoop(NetEventLoop eventLoop) {
    }

    default NetBufferFactory getBufferFactory() {
        return NetBufferFactory.INSTANCE;
    }

    default DataBufferFactory getDataBufferFactory() {
        return DataBufferFactory.getConcurrentFactory();
    }

    void close();

    void read();

    void write(NetBuffer data);
}
