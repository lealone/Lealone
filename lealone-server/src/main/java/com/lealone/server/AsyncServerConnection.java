/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import com.lealone.net.TransferConnection;
import com.lealone.net.WritableChannel;
import com.lealone.server.scheduler.ServerSessionInfo;

public abstract class AsyncServerConnection extends TransferConnection {

    public AsyncServerConnection(WritableChannel writableChannel) {
        super(writableChannel, true);
    }

    public abstract void closeSession(ServerSessionInfo si);

    public abstract int getSessionCount();

}
