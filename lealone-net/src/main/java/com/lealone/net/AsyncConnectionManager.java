/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.channels.ServerSocketChannel;

import com.lealone.db.scheduler.Scheduler;

public interface AsyncConnectionManager {

    AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer,
            Scheduler scheduler);

    void removeConnection(AsyncConnection conn);

    default void registerAccepter(ServerSocketChannel serverChannel) {
    }
}
