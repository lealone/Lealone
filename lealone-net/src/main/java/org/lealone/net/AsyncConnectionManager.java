/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.nio.channels.ServerSocketChannel;

public interface AsyncConnectionManager {

    AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer,
            Object scheduler);

    void removeConnection(AsyncConnection conn);

    default void registerAccepter(ServerSocketChannel serverChannel) {
    }
}
