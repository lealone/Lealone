/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;

import com.lealone.net.NetBuffer.WritableBuffer;

public interface WritableChannel {

    String getHost();

    int getPort();

    String getLocalHost();

    int getLocalPort();

    void setInputBuffer(NetBuffer inputBuffer);

    default List<WritableBuffer> getBuffers() {
        return null;
    }

    default void addBuffer(WritableBuffer buffer) {
    }

    default boolean isBio() {
        return false;
    }

    default SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException("getSocketChannel");
    }

    default NetEventLoop getEventLoop() {
        return null;
    }

    default void setEventLoop(NetEventLoop eventLoop) {
    }

    default SelectionKey getSelectionKey() {
        return null;
    }

    default void setSelectionKey(SelectionKey selectionKey) {
    }

    boolean isClosed();

    void close();

    void read();

    void write(WritableBuffer buffer);
}
