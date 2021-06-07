/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.nio.channels.SocketChannel;

public interface WritableChannel {

    void write(Object data);

    void close();

    String getHost();

    int getPort();

    default SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException("getSocketChannel");
    }

    NetBufferFactory getBufferFactory();

}
