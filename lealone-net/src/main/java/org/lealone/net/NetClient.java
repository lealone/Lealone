/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.net.InetSocketAddress;
import java.util.Map;

import org.lealone.db.async.Future;

public interface NetClient {

    default Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node) {
        return createConnection(config, node, null);
    }

    Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager);

    void addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn);

    void removeConnection(AsyncConnection conn);

    void close();

    boolean isClosed();

    boolean isThreadSafe();

}
