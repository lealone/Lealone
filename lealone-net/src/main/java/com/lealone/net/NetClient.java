/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.net.InetSocketAddress;
import java.util.Map;

import com.lealone.db.async.Future;
import com.lealone.db.scheduler.Scheduler;

public interface NetClient {

    default Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            Scheduler scheduler) {
        return createConnection(config, node, null, scheduler);
    }

    Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, Scheduler scheduler);

    AsyncConnection getConnection(InetSocketAddress inetSocketAddress);

    void addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn);

    void removeConnection(AsyncConnection conn);

    void removeConnection(InetSocketAddress inetSocketAddress);

    void close();

    boolean isClosed();

    void checkTimeout(long currentTime);

}
