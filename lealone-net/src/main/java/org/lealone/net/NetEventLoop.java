/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.lealone.db.DataBufferFactory;

public interface NetEventLoop {

    Object getOwner();

    void setOwner(Object owner);

    void setPreferBatchWrite(boolean preferBatchWrite);

    DataBufferFactory getDataBufferFactory();

    Selector getSelector();

    void select() throws IOException;

    void select(long timeout) throws IOException;

    void register(AsyncConnection conn);

    void wakeup();

    void addSocketChannel(SocketChannel channel);

    void addNetBuffer(SocketChannel channel, NetBuffer netBuffer);

    void read(SelectionKey key);

    void write();

    void write(SelectionKey key);

    void setNetClient(NetClient netClient);

    void setAccepter(Accepter accepter);

    void handleSelectedKeys();

    void closeChannel(SocketChannel channel);

    void close();

    boolean isInLoop();

    interface Accepter {
        void accept(SelectionKey key);
    }

    boolean isQueueLarge();
}
