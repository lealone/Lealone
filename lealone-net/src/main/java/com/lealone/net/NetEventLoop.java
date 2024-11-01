/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import com.lealone.db.DataBufferFactory;

public interface NetEventLoop {

    void setNetClient(NetClient netClient);

    NetClient getNetClient();

    void setPreferBatchWrite(boolean preferBatchWrite);

    DataBufferFactory getDataBufferFactory();

    Selector getSelector();

    void select() throws IOException;

    void select(long timeout) throws IOException;

    void register(AsyncConnection conn);

    void wakeUp();

    void addChannel(WritableChannel channel);

    void read(SelectionKey key);

    void write();

    void write(SelectionKey key);

    void write(WritableChannel channel, NetBuffer buffer);

    void handleSelectedKeys();

    void closeChannel(WritableChannel channel);

    void close();

    boolean isInLoop();

    boolean needWriteImmediately();

    void incrementPacketCount();

    void decrementPacketCount();

}
