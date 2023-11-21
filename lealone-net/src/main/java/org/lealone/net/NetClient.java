/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.net.InetSocketAddress;
import java.util.Map;

import org.lealone.db.async.Future;
import org.lealone.db.scheduler.Scheduler;

public interface NetClient {

    default Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            Scheduler scheduler) {
        return createConnection(config, node, null, scheduler);
    }

    Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, Scheduler scheduler);

    void addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn);

    void removeConnection(AsyncConnection conn);

    void close();

    boolean isClosed();

    boolean isThreadSafe();

    void checkTimeout(long currentTime);

}
