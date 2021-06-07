/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.server.ProtocolServer;

public interface NetServer extends ProtocolServer {

    void setConnectionManager(AsyncConnectionManager connectionManager);

}
