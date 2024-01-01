/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import com.lealone.net.TransferConnection;
import com.lealone.net.WritableChannel;
import com.lealone.server.scheduler.SessionInfo;

public abstract class AsyncServerConnection extends TransferConnection {

    public AsyncServerConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    public abstract void closeSession(SessionInfo si);

    public abstract int getSessionCount();

}
