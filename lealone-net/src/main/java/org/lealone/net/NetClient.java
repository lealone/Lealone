/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Map;

import org.lealone.db.async.Future;

public interface NetClient {

    Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node);

    Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager);

    void removeConnection(AsyncConnection conn);

    void close();

    boolean isClosed();

}
