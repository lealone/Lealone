/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import com.lealone.server.ProtocolServer;

public interface NetServer extends ProtocolServer {

    void setConnectionManager(AsyncConnectionManager connectionManager);

}
